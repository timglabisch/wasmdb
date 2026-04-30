//! Emit generated code from a parsed [`Model`].
//!
//! Both modes produce one top-level `TokenStream` suitable for
//! `include!`-ing inside `mod __generated { ... }` in the caller crate.
//! Paths into user code use `crate::<mod_path>::...` absolute references.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::Type;

use tables_fieldtypes::{classify, pascal_to_snake, FieldKind};

use crate::parse::{Model, Module, Query, Row};
use crate::CodegenError;

pub fn emit_client(
    model: &Model,
    url: &str,
    wasm_bindings: bool,
) -> Result<TokenStream, CodegenError> {
    let modules = model
        .modules
        .iter()
        .map(|m| emit_client_module(m, url))
        .collect::<Result<Vec<_>, _>>()?;
    let wasm = if wasm_bindings {
        let bindings = model
            .modules
            .iter()
            .flat_map(|m| m.queries.iter().map(move |q| emit_wasm_binding(m, q, url)));
        quote! { #(#bindings)* }
    } else {
        quote! {}
    };
    let register_all = emit_register_all_requirements(model, url)?;
    Ok(quote! {
        #(#modules)*
        #wasm
        #register_all
    })
}

pub fn emit_server(model: &Model, ctx_ty_str: &str) -> Result<TokenStream, CodegenError> {
    let ctx_ty: Type = syn::parse_str(ctx_ty_str)
        .map_err(|e| CodegenError::Emit(format!("ctx_type `{ctx_ty_str}`: {e}")))?;

    let modules = model
        .modules
        .iter()
        .map(|m| emit_server_module(m, &ctx_ty))
        .collect::<Result<Vec<_>, _>>()?;

    let register_calls = model
        .modules
        .iter()
        .flat_map(|m| {
            let mod_path = module_path_tokens(&m.path);
            m.queries.iter().map(move |q| {
                let reg = format_ident!("register_{}", q.fn_name);
                quote! { #mod_path::#reg(registry); }
            })
        })
        .collect::<Vec<_>>();

    Ok(quote! {
        #(#modules)*

        pub fn register_all(registry: &mut ::tables_storage::Registry<#ctx_ty>) {
            #(#register_calls)*
        }
    })
}

fn emit_client_module(m: &Module, url: &str) -> Result<TokenStream, CodegenError> {
    let rows = m
        .rows
        .iter()
        .map(emit_client_row)
        .collect::<Result<Vec<_>, _>>()?;
    let queries = m
        .queries
        .iter()
        .map(|q| emit_client_query(q, url))
        .collect::<Result<Vec<_>, _>>()?;
    let content = quote! {
        #(#rows)*
        #(#queries)*
    };
    Ok(wrap_in_mods(&m.path, module_prelude(m), content))
}

fn emit_server_module(m: &Module, ctx_ty: &Type) -> Result<TokenStream, CodegenError> {
    let user_mod = user_mod_path(&m.path);
    let queries = m
        .queries
        .iter()
        .map(|q| emit_server_query(q, ctx_ty, &user_mod))
        .collect::<Result<Vec<_>, _>>()?;
    let content = quote! {
        #(#queries)*
    };
    Ok(wrap_in_mods(&m.path, module_prelude(m), content))
}

/// Build the `use` prelude needed for a generated module to compile.
///
/// Today the only candidate is `Uuid`: `#[row]`/`#[query]` types are
/// emitted verbatim, so any field typed `Uuid` (or `Option<Uuid>`) needs
/// the newtype in scope. Other supported types (`i64`, `String`,
/// `Option<...>`) resolve via the prelude already.
///
/// `#[allow(unused_imports)]` keeps server-only modules quiet: those
/// emit only the params struct (no row), so the prelude can be dead even
/// when a sibling row would need it on the client side.
fn module_prelude(m: &Module) -> TokenStream {
    let needs_uuid = m.rows.iter().any(|r| row_uses_uuid(r))
        || m.queries.iter().any(|q| query_uses_uuid(q));
    if needs_uuid {
        quote! { #[allow(unused_imports)] use ::sql_engine::storage::Uuid; }
    } else {
        quote! {}
    }
}

fn row_uses_uuid(row: &Row) -> bool {
    row.fields.iter().any(|(_, t)| type_is_uuid(t))
}

fn query_uses_uuid(q: &Query) -> bool {
    q.params.iter().any(|(_, t)| type_is_uuid(t))
}

fn type_is_uuid(ty: &Type) -> bool {
    matches!(classify(ty), Some(FieldKind::Uuid) | Some(FieldKind::OptUuid))
}

fn emit_client_row(row: &Row) -> Result<TokenStream, CodegenError> {
    let name = format_ident!("{}", row.name);
    let pk_name = format_ident!("{}", row.pk_name);
    let pk_ty = &row.pk_ty;
    let field_defs = row.fields.iter().map(|(n, t)| {
        let ident = format_ident!("{n}");
        quote! { pub #ident: #t }
    });
    let dbtable_impl = emit_dbtable_impl(
        &row.name,
        &row.fields,
        &row.pk_name,
        row.table_name.as_deref(),
    )?;

    Ok(quote! {
        #[derive(
            ::core::fmt::Debug,
            ::core::clone::Clone,
            ::borsh::BorshSerialize,
            ::borsh::BorshDeserialize,
            ::serde::Serialize,
            ::serde::Deserialize,
        )]
        pub struct #name {
            #(#field_defs,)*
        }

        impl ::tables::Row for #name {
            type Pk = #pk_ty;
            fn pk(&self) -> Self::Pk {
                ::core::clone::Clone::clone(&self.#pk_name)
            }
        }

        #dbtable_impl
    })
}

/// Classify each field and emit `impl ::sql_engine::DbTable for #struct_name`.
/// `struct_name` is the Rust identifier; the `TABLE` const becomes its
/// snake_case form. Used by both the client-side row duplicate and any
/// future server-side path that regenerates rows.
fn emit_dbtable_impl(
    struct_name: &str,
    fields: &[(String, Type)],
    pk_name: &str,
    table_name: Option<&str>,
) -> Result<TokenStream, CodegenError> {
    let name = format_ident!("{}", struct_name);
    let table_lit = table_name
        .map(ToString::to_string)
        .unwrap_or_else(|| pascal_to_snake(struct_name));

    let mut column_defs = Vec::with_capacity(fields.len());
    let mut cell_pushes = Vec::with_capacity(fields.len());
    let mut pk_idx: Option<usize> = None;

    for (idx, (field_name, ty)) in fields.iter().enumerate() {
        let kind = classify(ty).ok_or_else(|| {
            CodegenError::Emit(format!(
                "row `{struct_name}` field `{field_name}`: supported types are i64, String, Option<i64>, Option<String>",
            ))
        })?;
        let (dt_tokens, nullable) = kind.column_meta();
        column_defs.push(quote! {
            ::sql_engine::schema::ColumnSchema {
                name: #field_name.into(),
                data_type: #dt_tokens,
                nullable: #nullable,
            }
        });

        let ident = format_ident!("{field_name}");
        cell_pushes.push(match kind {
            FieldKind::I64 => quote! {
                out.push(::sql_engine::storage::CellValue::I64(self.#ident));
            },
            FieldKind::Str => quote! {
                out.push(::sql_engine::storage::CellValue::Str(self.#ident));
            },
            FieldKind::Uuid => quote! {
                out.push(::sql_engine::storage::CellValue::Uuid(self.#ident.0));
            },
            FieldKind::OptI64 => quote! {
                out.push(match self.#ident {
                    ::core::option::Option::Some(v) => ::sql_engine::storage::CellValue::I64(v),
                    ::core::option::Option::None => ::sql_engine::storage::CellValue::Null,
                });
            },
            FieldKind::OptStr => quote! {
                out.push(match self.#ident {
                    ::core::option::Option::Some(v) => ::sql_engine::storage::CellValue::Str(v),
                    ::core::option::Option::None => ::sql_engine::storage::CellValue::Null,
                });
            },
            FieldKind::OptUuid => quote! {
                out.push(match self.#ident {
                    ::core::option::Option::Some(v) => ::sql_engine::storage::CellValue::Uuid(v.0),
                    ::core::option::Option::None => ::sql_engine::storage::CellValue::Null,
                });
            },
        });

        if field_name == pk_name {
            pk_idx = Some(idx);
        }
    }

    let pk_idx = pk_idx.ok_or_else(|| {
        CodegenError::Emit(format!(
            "row `{struct_name}`: #[pk] field `{pk_name}` not present in fields",
        ))
    })?;
    let n_fields = fields.len();

    Ok(quote! {
        impl ::sql_engine::DbTable for #name {
            const TABLE: &'static str = #table_lit;

            fn schema() -> ::sql_engine::schema::TableSchema {
                ::sql_engine::schema::TableSchema {
                    name: <Self as ::sql_engine::DbTable>::TABLE.into(),
                    columns: ::std::vec![ #(#column_defs),* ],
                    primary_key: ::std::vec![ #pk_idx ],
                    indexes: ::std::vec![],
                }
            }

            fn into_cells(self) -> ::std::vec::Vec<::sql_engine::storage::CellValue> {
                let mut out = ::std::vec::Vec::with_capacity(#n_fields);
                #(#cell_pushes)*
                out
            }
        }
    })
}

fn emit_client_query(q: &Query, url: &str) -> Result<TokenStream, CodegenError> {
    let fn_name = format_ident!("{}", q.fn_name);
    let marker = format_ident!("{}", q.marker_name);
    let id_lit = &q.id;
    let row_ty = &q.row_ty;

    let param_fields = q.params.iter().map(|(n, t)| {
        let ident = format_ident!("{n}");
        quote! { pub #ident: #t }
    });
    let param_args = q.params.iter().map(|(n, t)| {
        let ident = format_ident!("{n}");
        quote! { #ident: #t }
    });
    let param_names: Vec<_> = q.params.iter().map(|(n, _)| format_ident!("{n}")).collect();
    let param_defs = emit_param_defs(&q.fn_name, &q.params)?;

    Ok(quote! {
        #[derive(
            ::core::fmt::Debug,
            ::core::clone::Clone,
            ::borsh::BorshSerialize,
            ::borsh::BorshDeserialize,
            ::serde::Serialize,
            ::serde::Deserialize,
        )]
        pub struct #marker {
            #(#param_fields,)*
        }

        impl ::tables::Fetcher for #marker {
            const ID: ::tables::FetcherId = #id_lit;
            type Params = Self;
            type Row = #row_ty;
        }

        impl ::requirements::DbRequirement for #marker {
            const ID: &'static str = #id_lit;
            type Row = #row_ty;

            fn meta() -> ::requirements::RequirementMeta {
                ::requirements::RequirementMeta {
                    row_table: <<Self as ::requirements::DbRequirement>::Row as ::sql_engine::DbTable>::TABLE.into(),
                    params: ::std::vec![ #(#param_defs),* ],
                }
            }
        }

        pub async fn #fn_name(#(#param_args),*)
            -> ::core::result::Result<
                ::std::vec::Vec<#row_ty>,
                ::tables_client::FetchError,
            >
        {
            ::tables_client::fetch::<#marker>(
                #url,
                #marker { #(#param_names),* },
            ).await
        }
    })
}

fn emit_wasm_binding(m: &Module, q: &Query, url: &str) -> TokenStream {
    let marker = format_ident!("{}", q.marker_name);
    let mod_path_tokens = module_path_tokens(&m.path);
    let mod_joined = m.path.join("_");
    let wasm_fn = format_ident!("fetch_{}_{}", mod_joined, q.fn_name);

    quote! {
        #[cfg(target_arch = "wasm32")]
        #[::wasm_bindgen::prelude::wasm_bindgen]
        pub async fn #wasm_fn(
            params: ::wasm_bindgen::JsValue,
        ) -> ::core::result::Result<::wasm_bindgen::JsValue, ::wasm_bindgen::JsError> {
            let p: #mod_path_tokens::#marker =
                ::serde_wasm_bindgen::from_value(params)
                    .map_err(|e| ::wasm_bindgen::JsError::new(&::std::string::ToString::to_string(&e)))?;
            let rows = ::tables_client::fetch::<#mod_path_tokens::#marker>(#url, p)
                .await
                .map_err(|e| ::wasm_bindgen::JsError::new(&::std::string::ToString::to_string(&e)))?;
            ::serde_wasm_bindgen::to_value(&rows)
                .map_err(|e| ::wasm_bindgen::JsError::new(&::std::string::ToString::to_string(&e)))
        }
    }
}

fn emit_server_query(
    q: &Query,
    ctx_ty: &Type,
    user_mod: &TokenStream,
) -> Result<TokenStream, CodegenError> {
    let fn_name = format_ident!("{}", q.fn_name);
    let marker = format_ident!("{}", q.marker_name);
    let id_lit = &q.id;
    let register_ident = format_ident!("register_{}", q.fn_name);
    let row_ty = &q.row_ty;

    let param_fields = q.params.iter().map(|(n, t)| {
        let ident = format_ident!("{n}");
        quote! { pub #ident: #t }
    });
    let param_idents: Vec<_> = q
        .params
        .iter()
        .map(|(n, _)| format_ident!("{n}"))
        .collect();
    let param_defs = emit_param_defs(&q.fn_name, &q.params)?;
    let row_path = quote! { #user_mod::#row_ty };

    Ok(quote! {
        #[derive(
            ::core::fmt::Debug,
            ::core::clone::Clone,
            ::borsh::BorshSerialize,
            ::borsh::BorshDeserialize,
            ::serde::Serialize,
            ::serde::Deserialize,
        )]
        pub struct #marker {
            #(#param_fields,)*
        }

        impl ::tables::Fetcher for #marker {
            const ID: ::tables::FetcherId = #id_lit;
            type Params = Self;
            type Row = #row_path;
        }

        impl ::requirements::DbRequirement for #marker {
            const ID: &'static str = #id_lit;
            type Row = #row_path;

            fn meta() -> ::requirements::RequirementMeta {
                ::requirements::RequirementMeta {
                    row_table: <<Self as ::requirements::DbRequirement>::Row as ::sql_engine::DbTable>::TABLE.into(),
                    params: ::std::vec![ #(#param_defs),* ],
                }
            }
        }

        pub fn #register_ident(registry: &mut ::tables_storage::Registry<#ctx_ty>) {
            registry.register::<#marker>(|#[allow(unused_variables)] params, ctx| {
                ::std::boxed::Box::pin(async move {
                    #user_mod::#fn_name(#(params.#param_idents,)* ctx).await
                        .map_err(|e| ::tables_storage::StorageError::Storage(
                            ::std::string::ToString::to_string(&e),
                        ))
                })
            });
        }
    })
}

/// Emit a single top-level `register_all_requirements` fn that wires
/// every codegen-emitted query marker into a `RequirementRegistry`. Each
/// closure shares the embedder-supplied `apply` function, fetches via
/// HTTP, builds a `ZSet` from the typed rows, and applies it — the
/// embedder decides which database receives the write.
fn emit_register_all_requirements(model: &Model, url: &str) -> Result<TokenStream, CodegenError> {
    let mut blocks = Vec::new();
    for module in &model.modules {
        let mod_path = module_path_tokens(&module.path);
        for q in &module.queries {
            let marker = format_ident!("{}", q.marker_name);
            let row_ty = &q.row_ty;
            let param_names: Vec<_> =
                q.params.iter().map(|(n, _)| format_ident!("{n}")).collect();
            let arg_bindings = emit_arg_bindings(&q.fn_name, &q.params)?;
            blocks.push(quote! {
                {
                    let apply_q = ::std::clone::Clone::clone(&apply);
                    let fetcher: ::requirements::RequirementFn = ::std::sync::Arc::new(move |args| {
                        let apply = ::std::clone::Clone::clone(&apply_q);
                        ::std::boxed::Box::pin(async move {
                            #(#arg_bindings)*
                            let params = #mod_path::#marker { #(#param_names),* };
                            let rows = ::tables_client::fetch::<#mod_path::#marker>(#url, params).await
                                .map_err(|e| ::std::string::ToString::to_string(&e))?;
                            let mut zset = ::sql_engine::storage::ZSet::new();
                            for row in rows {
                                zset.insert(
                                    <#mod_path::#row_ty as ::sql_engine::DbTable>::TABLE.into(),
                                    <#mod_path::#row_ty as ::sql_engine::DbTable>::into_cells(row),
                                );
                            }
                            apply(&zset)?;
                            ::core::result::Result::Ok(())
                        })
                    });
                    registry.insert(::requirements::Requirement::new(
                        <#mod_path::#marker as ::requirements::DbRequirement>::ID,
                        <#mod_path::#marker as ::requirements::DbRequirement>::meta(),
                        fetcher,
                    ));
                }
            });
        }
    }
    Ok(quote! {
        #[cfg(target_arch = "wasm32")]
        pub fn register_all_requirements(
            apply: ::std::rc::Rc<dyn ::core::ops::Fn(&::sql_engine::storage::ZSet) -> ::core::result::Result<(), ::std::string::String>>,
            registry: &mut ::requirements::RequirementRegistry,
        ) {
            #(#blocks)*
        }
    })
}

/// Build `RequirementParamDef` entries for a query's params.
fn emit_param_defs(
    fn_name: &str,
    params: &[(String, Type)],
) -> Result<Vec<TokenStream>, CodegenError> {
    params
        .iter()
        .map(|(name, ty)| {
            let kind = classify(ty).ok_or_else(|| {
                CodegenError::Emit(format!(
                    "query `{fn_name}` param `{name}`: supported types are i64, String, Option<i64>, Option<String>",
                ))
            })?;
            let (dt_tokens, _nullable) = kind.column_meta();
            Ok(quote! {
                ::requirements::RequirementParamDef {
                    name: #name.into(),
                    data_type: #dt_tokens,
                }
            })
        })
        .collect()
}

/// Emit a positional `args`-to-typed-param binding block. Errors stringify
/// the problematic argument position so Phase 0 failure messages keep
/// their caller context.
fn emit_arg_bindings(
    fn_name: &str,
    params: &[(String, Type)],
) -> Result<Vec<TokenStream>, CodegenError> {
    params
        .iter()
        .enumerate()
        .map(|(idx, (name, ty))| {
            let kind = classify(ty).ok_or_else(|| {
                CodegenError::Emit(format!(
                    "query `{fn_name}` param `{name}`: supported types are i64, String, Option<i64>, Option<String>",
                ))
            })?;
            let ident = format_ident!("{name}");
            let idx_lit = idx;
            let err_prefix = format!("arg {idx} ({name}): ");
            let err_int = format!("{err_prefix}expected Int");
            let err_text = format!("{err_prefix}expected Text");
            let err_uuid = format!("{err_prefix}expected Uuid");
            let err_non_null_int = format!("{err_prefix}expected Int, got NULL");
            let err_non_null_text = format!("{err_prefix}expected Text, got NULL");
            let err_non_null_uuid = format!("{err_prefix}expected Uuid, got NULL");

            let binding = match kind {
                FieldKind::I64 => quote! {
                    let #ident: i64 = match args.get(#idx_lit) {
                        ::core::option::Option::Some(::sql_parser::ast::Value::Int(v)) => *v,
                        ::core::option::Option::Some(::sql_parser::ast::Value::Null) => {
                            return ::core::result::Result::Err(#err_non_null_int.into());
                        }
                        _ => return ::core::result::Result::Err(#err_int.into()),
                    };
                },
                FieldKind::Str => quote! {
                    let #ident: ::std::string::String = match args.get(#idx_lit) {
                        ::core::option::Option::Some(::sql_parser::ast::Value::Text(v)) => v.clone(),
                        ::core::option::Option::Some(::sql_parser::ast::Value::Null) => {
                            return ::core::result::Result::Err(#err_non_null_text.into());
                        }
                        _ => return ::core::result::Result::Err(#err_text.into()),
                    };
                },
                FieldKind::Uuid => quote! {
                    let #ident: ::sql_engine::storage::Uuid = match args.get(#idx_lit) {
                        ::core::option::Option::Some(::sql_parser::ast::Value::Uuid(b)) => ::sql_engine::storage::Uuid(*b),
                        ::core::option::Option::Some(::sql_parser::ast::Value::Null) => {
                            return ::core::result::Result::Err(#err_non_null_uuid.into());
                        }
                        _ => return ::core::result::Result::Err(#err_uuid.into()),
                    };
                },
                FieldKind::OptI64 => quote! {
                    let #ident: ::core::option::Option<i64> = match args.get(#idx_lit) {
                        ::core::option::Option::Some(::sql_parser::ast::Value::Int(v)) => ::core::option::Option::Some(*v),
                        ::core::option::Option::Some(::sql_parser::ast::Value::Null) => ::core::option::Option::None,
                        _ => return ::core::result::Result::Err(#err_int.into()),
                    };
                },
                FieldKind::OptStr => quote! {
                    let #ident: ::core::option::Option<::std::string::String> = match args.get(#idx_lit) {
                        ::core::option::Option::Some(::sql_parser::ast::Value::Text(v)) => ::core::option::Option::Some(v.clone()),
                        ::core::option::Option::Some(::sql_parser::ast::Value::Null) => ::core::option::Option::None,
                        _ => return ::core::result::Result::Err(#err_text.into()),
                    };
                },
                FieldKind::OptUuid => quote! {
                    let #ident: ::core::option::Option<::sql_engine::storage::Uuid> = match args.get(#idx_lit) {
                        ::core::option::Option::Some(::sql_parser::ast::Value::Uuid(b)) => ::core::option::Option::Some(::sql_engine::storage::Uuid(*b)),
                        ::core::option::Option::Some(::sql_parser::ast::Value::Null) => ::core::option::Option::None,
                        _ => return ::core::result::Result::Err(#err_uuid.into()),
                    };
                },
            };
            Ok(binding)
        })
        .collect()
}

fn wrap_in_mods(path: &[String], prelude: TokenStream, content: TokenStream) -> TokenStream {
    let mut ts = quote! { #prelude #content };
    for name in path.iter().rev() {
        let ident = format_ident!("{name}");
        ts = quote! { pub mod #ident { #ts } };
    }
    ts
}

/// Path to a generated module, relative to the `include!` root.
fn module_path_tokens(path: &[String]) -> TokenStream {
    let idents: Vec<_> = path.iter().map(|s| format_ident!("{s}")).collect();
    quote! { #(#idents)::* }
}

/// Emit a TypeScript file describing the typed requirement builders. One
/// builder fn per `#[query]`, grouped by module path. The resulting object
/// is consumed by the React `useQuery({sql, requires})` overload to construct
/// `requires` entries with field-level type safety. Hand-rolled output (no
/// `prettyplease` for TS) — keep formatting conservative.
pub fn emit_ts_requirements(model: &Model) -> String {
    let mut tree = TsTree::new();
    for module in &model.modules {
        for q in &module.queries {
            tree.insert(&module.path, q);
        }
    }
    let mut out = String::new();
    out.push_str(
        "// AUTO-GENERATED by tables-codegen. Do not edit.\n\
         // Builders for typed requirements consumed by `useQuery({sql, requires})`.\n\n\
         export type RequirementArg = number | string | null;\n\n\
         export interface RequirementSpec {\n  id: string;\n  args: RequirementArg[];\n}\n\n",
    );
    out.push_str("export const requirements = ");
    tree.write(&mut out, 0);
    out.push_str(" as const;\n");
    out
}

struct TsTree {
    children: std::collections::BTreeMap<String, TsTree>,
    queries: Vec<TsQueryEntry>,
}

struct TsQueryEntry {
    fn_name: String,
    id: String,
    params: Vec<(String, FieldKind)>,
}

impl TsTree {
    fn new() -> Self {
        Self {
            children: std::collections::BTreeMap::new(),
            queries: Vec::new(),
        }
    }

    fn insert(&mut self, path: &[String], q: &Query) {
        let params: Vec<(String, FieldKind)> = q
            .params
            .iter()
            .filter_map(|(n, t)| classify(t).map(|k| (n.clone(), k)))
            .collect();
        let entry = TsQueryEntry {
            fn_name: q.fn_name.clone(),
            id: q.id.clone(),
            params,
        };
        if path.is_empty() {
            self.queries.push(entry);
            return;
        }
        let head = &path[0];
        let child = self
            .children
            .entry(head.clone())
            .or_insert_with(TsTree::new);
        child.insert(&path[1..], q);
    }

    fn write(&self, out: &mut String, depth: usize) {
        let pad = "  ".repeat(depth);
        let inner_pad = "  ".repeat(depth + 1);
        out.push_str("{\n");
        let mut first = true;
        for (name, child) in &self.children {
            if !first {
                out.push_str(",\n");
            }
            first = false;
            out.push_str(&inner_pad);
            out.push_str(&snake_to_camel(name));
            out.push_str(": ");
            child.write(out, depth + 1);
        }
        for q in &self.queries {
            if !first {
                out.push_str(",\n");
            }
            first = false;
            out.push_str(&inner_pad);
            write_ts_query_builder(out, q);
        }
        if !first {
            out.push('\n');
        }
        out.push_str(&pad);
        out.push('}');
    }
}

fn write_ts_query_builder(out: &mut String, q: &TsQueryEntry) {
    let params_signature = q
        .params
        .iter()
        .map(|(n, k)| format!("{}: {}", snake_to_camel(n), ts_arg_type(*k)))
        .collect::<Vec<_>>()
        .join(", ");
    let args_array = q
        .params
        .iter()
        .map(|(n, _)| snake_to_camel(n))
        .collect::<Vec<_>>()
        .join(", ");
    out.push_str(&snake_to_camel(&q.fn_name));
    out.push_str(": (");
    out.push_str(&params_signature);
    out.push_str("): RequirementSpec => ({ id: '");
    out.push_str(&q.id);
    out.push_str("', args: [");
    out.push_str(&args_array);
    out.push_str("] })");
}

fn ts_arg_type(k: FieldKind) -> &'static str {
    match k {
        FieldKind::I64 => "number",
        FieldKind::Str | FieldKind::Uuid => "string",
        FieldKind::OptI64 => "number | null",
        FieldKind::OptStr | FieldKind::OptUuid => "string | null",
    }
}

fn snake_to_camel(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut upper_next = false;
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

/// Path to the user's module (where the query fn + row struct live),
/// absolute from the crate root.
fn user_mod_path(path: &[String]) -> TokenStream {
    let idents: Vec<_> = path.iter().map(|s| format_ident!("{s}")).collect();
    quote! { crate::#(#idents)::* }
}
