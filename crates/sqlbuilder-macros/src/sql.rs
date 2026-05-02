use std::collections::{HashMap, HashSet};

use proc_macro2::TokenStream;
use quote::{quote, quote_spanned};
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{Expr, Ident, LitStr, Token};

use crate::parse::collect_placeholders;

/// Parsed input of `sql!("... {name} ...", name = expr, ...)`.
struct SqlInput {
    sql: LitStr,
    bindings: Vec<(Ident, Expr)>,
}

impl Parse for SqlInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let sql: LitStr = input.parse()?;
        let mut bindings = Vec::new();
        if input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
            let parsed: Punctuated<Binding, Token![,]> =
                Punctuated::parse_terminated(input)?;
            for b in parsed {
                bindings.push((b.name, b.value));
            }
        }
        Ok(SqlInput { sql, bindings })
    }
}

struct Binding {
    name: Ident,
    value: Expr,
}

impl Parse for Binding {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let name: Ident = input.parse()?;
        input.parse::<Token![=]>()?;
        let value: Expr = input.parse()?;
        Ok(Binding { name, value })
    }
}

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let SqlInput { sql, bindings } = syn::parse2(input)?;
    let sql_text = sql.value();
    let placeholders = collect_placeholders(&sql_text, sql.span())
        .map_err(|e| syn::Error::new(sql.span(), e.message))?;

    if placeholders.is_empty() && !bindings.is_empty() {
        return Err(syn::Error::new(
            sql.span(),
            "sql! has explicit bindings but the SQL string has no `{placeholder}`",
        ));
    }

    // Validate explicit bindings: no duplicates, every one is referenced.
    let placeholder_set: HashSet<&str> =
        placeholders.iter().map(|s| s.as_str()).collect();
    let mut explicit: HashMap<String, Expr> = HashMap::new();
    for (name, value) in bindings {
        let key = name.to_string();
        if explicit.contains_key(&key) {
            return Err(syn::Error::new(
                name.span(),
                format!("duplicate binding `{key}` in sql! call"),
            ));
        }
        if !placeholder_set.contains(key.as_str()) {
            return Err(syn::Error::new(
                name.span(),
                format!("binding `{key}` does not appear as `{{{key}}}` in the SQL string"),
            ));
        }
        explicit.insert(key, value);
    }

    // Each unique placeholder gets one entry in `parts`. For explicit
    // bindings we use the bound expression; otherwise we parse the name as
    // a Rust expression so dotted paths like `self.id` capture as field
    // access. Plain idents take the same path (Ident is a valid Expr).
    let mut seen = HashSet::new();
    let mut part_inits: Vec<TokenStream> = Vec::new();
    let mut sidecar_exprs: Vec<TokenStream> = Vec::new();
    for name in &placeholders {
        if !seen.insert(name.clone()) {
            continue;
        }
        let value_expr: TokenStream = if let Some(expr) = explicit.remove(name) {
            quote! { #expr }
        } else {
            let parsed = syn::parse_str::<Expr>(name).map_err(|_| {
                syn::Error::new(
                    sql.span(),
                    format!(
                        "placeholder `{{{name}}}` has no binding and is not a valid Rust expression \
                         to capture from scope; pass `<ident> = <expr>` explicitly"
                    ),
                )
            })?;
            quote! { #parsed }
        };
        sidecar_exprs.push(value_expr.clone());
        part_inits.push(quote! {
            (
                ::std::string::String::from(#name),
                ::sqlbuilder::IntoSqlPart::into_sql_part(&(#value_expr)),
            )
        });
    }

    // Sidecar: dead code (the closure is never called) referencing each
    // bound expression by borrow. Gives rust-analyzer a real syntactic site
    // to resolve `self.id` / `o.inner.value` against — enabling Goto, Hover,
    // Find Usages, and silencing spurious "field never read" warnings.
    // `quote_spanned!` puts compile errors near the SQL string literal.
    let sidecar = quote_spanned! { sql.span() =>
        let _sqlbuilder_sidecar = || {
            #( let _ = &(#sidecar_exprs); )*
        };
    };

    let count = part_inits.len();
    Ok(quote! {
        {
            #sidecar
            ::sqlbuilder::SqlStmt {
                sql: ::std::string::String::from(#sql_text),
                parts: {
                    let mut __v: ::std::vec::Vec<(
                        ::std::string::String,
                        ::sqlbuilder::SqlPart,
                    )> = ::std::vec::Vec::with_capacity(#count);
                    #( __v.push(#part_inits); )*
                    __v
                },
            }
        }
    })
}
