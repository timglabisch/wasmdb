//! Parse a server-crate source tree, collect `#[row]` structs and
//! `#[query]` async fns per module.
//!
//! Entry: [`scan`]. Walks the module tree starting from `<root>/lib.rs`
//! via `mod <name>;` declarations (finds `<name>.rs` or `<name>/mod.rs`).

use std::path::{Path, PathBuf};

use syn::{
    parse::ParseStream, Attribute, Fields, FnArg, GenericArgument, Ident, Item, ItemFn, ItemMod,
    ItemStruct, LitStr, Pat, PatType, PathArguments, ReturnType, Token, Type, TypePath,
};

use crate::CodegenError;

pub struct Model {
    pub modules: Vec<Module>,
    pub scanned_files: Vec<PathBuf>,
}

pub struct Module {
    /// Logical Rust path, e.g. `["customers"]` for `src/customers.rs`.
    pub path: Vec<String>,
    pub rows: Vec<Row>,
    pub queries: Vec<Query>,
}

pub struct Row {
    pub name: String,
    pub fields: Vec<(String, Type)>,
    pub pk_name: String,
    pub pk_ty: Type,
}

pub struct Query {
    /// `by_owner`
    pub fn_name: String,
    /// `ByOwner` — PascalCase of `fn_name`.
    pub marker_name: String,
    /// Explicit wire id from `#[query(id = "...")]`.
    pub id: String,
    /// Params = all fn args before the ctx arg (declaration order).
    pub params: Vec<(String, Type)>,
    /// Inner T from `Result<Vec<T>, _>`.
    pub row_ty: Type,
}

pub fn scan(root: &Path) -> Result<Model, CodegenError> {
    let lib_path = root.join("lib.rs");
    let content = std::fs::read_to_string(&lib_path)
        .map_err(|e| CodegenError::Io(e))?;
    let file = syn::parse_file(&content)
        .map_err(|e| CodegenError::Parse(format!("{}: {}", lib_path.display(), e)))?;

    let mut model = Model {
        modules: vec![],
        scanned_files: vec![lib_path.clone()],
    };

    walk_items(&file.items, root, &[], &mut model)?;
    Ok(model)
}

fn walk_items(
    items: &[Item],
    dir: &Path,
    mod_path: &[String],
    model: &mut Model,
) -> Result<(), CodegenError> {
    let mut rows = vec![];
    let mut queries = vec![];

    for item in items {
        match item {
            Item::Struct(s) if has_attr(&s.attrs, "row") => {
                rows.push(parse_row(s)?);
            }
            Item::Fn(f) => {
                if let Some(attr) = find_attr(&f.attrs, "query") {
                    queries.push(parse_query(f, attr)?);
                }
            }
            _ => {}
        }
    }

    if !rows.is_empty() || !queries.is_empty() {
        model.modules.push(Module {
            path: mod_path.to_vec(),
            rows,
            queries,
        });
    }

    for item in items {
        if let Item::Mod(m) = item {
            process_mod(m, dir, mod_path, model)?;
        }
    }

    Ok(())
}

fn process_mod(
    m: &ItemMod,
    dir: &Path,
    parent_path: &[String],
    model: &mut Model,
) -> Result<(), CodegenError> {
    let name = m.ident.to_string();
    let mut new_path = parent_path.to_vec();
    new_path.push(name.clone());

    if let Some((_, items)) = &m.content {
        walk_items(items, dir, &new_path, model)?;
        return Ok(());
    }

    // External module: <dir>/<name>.rs or <dir>/<name>/mod.rs
    let file_rs = dir.join(format!("{name}.rs"));
    let mod_rs = dir.join(&name).join("mod.rs");
    let (source, next_dir) = if file_rs.exists() {
        (file_rs, dir.join(&name))
    } else if mod_rs.exists() {
        let nd = dir.join(&name);
        (mod_rs, nd)
    } else {
        return Err(CodegenError::Parse(format!(
            "cannot find source for `mod {name};` under {}",
            dir.display()
        )));
    };

    let content = std::fs::read_to_string(&source)?;
    let file = syn::parse_file(&content)
        .map_err(|e| CodegenError::Parse(format!("{}: {}", source.display(), e)))?;
    model.scanned_files.push(source);

    walk_items(&file.items, &next_dir, &new_path, model)?;
    Ok(())
}

fn has_attr(attrs: &[Attribute], name: &str) -> bool {
    attrs.iter().any(|a| a.path().is_ident(name))
}

fn find_attr<'a>(attrs: &'a [Attribute], name: &str) -> Option<&'a Attribute> {
    attrs.iter().find(|a| a.path().is_ident(name))
}

fn parse_row(s: &ItemStruct) -> Result<Row, CodegenError> {
    let name = s.ident.to_string();
    let Fields::Named(named) = &s.fields else {
        return Err(CodegenError::Parse(format!(
            "#[row] on `{name}`: needs named fields"
        )));
    };

    let mut fields = vec![];
    let mut pk: Option<(String, Type)> = None;
    for f in &named.named {
        let Some(ident) = &f.ident else {
            return Err(CodegenError::Parse(format!("#[row] on `{name}`: unnamed field")));
        };
        let fname = ident.to_string();
        let is_pk = f.attrs.iter().any(|a| a.path().is_ident("pk"));
        if is_pk {
            if pk.is_some() {
                return Err(CodegenError::Parse(format!(
                    "#[row] on `{name}`: more than one #[pk]"
                )));
            }
            pk = Some((fname.clone(), f.ty.clone()));
        }
        fields.push((fname, f.ty.clone()));
    }
    let (pk_name, pk_ty) = pk.ok_or_else(|| {
        CodegenError::Parse(format!("#[row] on `{name}`: missing #[pk] field"))
    })?;

    Ok(Row {
        name,
        fields,
        pk_name,
        pk_ty,
    })
}

fn parse_query(f: &ItemFn, attr: &Attribute) -> Result<Query, CodegenError> {
    let fn_name = f.sig.ident.to_string();

    if f.sig.asyncness.is_none() {
        return Err(CodegenError::Parse(format!(
            "#[query] on `{fn_name}`: must be `async fn`"
        )));
    }

    let id = attr
        .parse_args_with(parse_id_arg)
        .map_err(|e| CodegenError::Parse(format!("#[query] on `{fn_name}`: {e}")))?;

    let marker_name = snake_to_pascal(&fn_name);

    let args: Vec<&FnArg> = f.sig.inputs.iter().collect();
    if args.len() < 2 {
        return Err(CodegenError::Parse(format!(
            "#[query] on `{fn_name}`: needs at least one param + ctx arg"
        )));
    }

    // Params: all args before the last.
    let mut params = vec![];
    for arg in &args[..args.len() - 1] {
        let FnArg::Typed(PatType { pat, ty, .. }) = arg else {
            return Err(CodegenError::Parse(format!(
                "#[query] on `{fn_name}`: unexpected `self` arg"
            )));
        };
        let Pat::Ident(pi) = pat.as_ref() else {
            return Err(CodegenError::Parse(format!(
                "#[query] on `{fn_name}`: param must be a simple ident"
            )));
        };
        params.push((pi.ident.to_string(), (**ty).clone()));
    }

    // Ctx is the last arg; must be `&T`. We don't use its type here —
    // the server build pins the ctx_type via Builder.
    let ctx_arg = args.last().unwrap();
    let FnArg::Typed(PatType { ty: ctx_ty, .. }) = ctx_arg else {
        return Err(CodegenError::Parse(format!(
            "#[query] on `{fn_name}`: ctx arg can't be `self`"
        )));
    };
    if !matches!(ctx_ty.as_ref(), Type::Reference(_)) {
        return Err(CodegenError::Parse(format!(
            "#[query] on `{fn_name}`: ctx arg must be `&T`"
        )));
    }

    // Return: `Result<Vec<Row>, _>`.
    let ReturnType::Type(_, ret) = &f.sig.output else {
        return Err(CodegenError::Parse(format!(
            "#[query] on `{fn_name}`: return must be `Result<Vec<Row>, _>`"
        )));
    };
    let row_ty = unwrap_result_vec(ret.as_ref()).ok_or_else(|| {
        CodegenError::Parse(format!(
            "#[query] on `{fn_name}`: return must be `Result<Vec<Row>, _>`"
        ))
    })?;

    Ok(Query {
        fn_name,
        marker_name,
        id,
        params,
        row_ty,
    })
}

fn parse_id_arg(input: ParseStream) -> syn::Result<String> {
    let ident: Ident = input.parse()?;
    if ident != "id" {
        return Err(syn::Error::new(ident.span(), "expected `id = \"...\"`"));
    }
    let _eq: Token![=] = input.parse()?;
    let lit: LitStr = input.parse()?;
    Ok(lit.value())
}

fn snake_to_pascal(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut upper_next = true;
    for c in s.chars() {
        if c == '_' {
            upper_next = true;
        } else if upper_next {
            out.extend(c.to_uppercase());
            upper_next = false;
        } else {
            out.push(c);
        }
    }
    out
}

fn unwrap_result_vec(ty: &Type) -> Option<Type> {
    let Type::Path(TypePath { path, .. }) = ty else {
        return None;
    };
    let last = path.segments.last()?;
    if last.ident != "Result" {
        return None;
    }
    let PathArguments::AngleBracketed(gargs) = &last.arguments else {
        return None;
    };
    let ok = gargs.args.iter().find_map(|a| match a {
        GenericArgument::Type(t) => Some(t),
        _ => None,
    })?;

    // ok must be Vec<T>
    let Type::Path(TypePath { path: vec_path, .. }) = ok else {
        return None;
    };
    let vec_seg = vec_path.segments.last()?;
    if vec_seg.ident != "Vec" {
        return None;
    }
    let PathArguments::AngleBracketed(vargs) = &vec_seg.arguments else {
        return None;
    };
    vargs.args.iter().find_map(|a| match a {
        GenericArgument::Type(t) => Some(t.clone()),
        _ => None,
    })
}
