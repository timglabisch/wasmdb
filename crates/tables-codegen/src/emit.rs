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
    Ok(quote! {
        #(#modules)*
        #wasm
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
    Ok(wrap_in_mods(&m.path, content))
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
    Ok(wrap_in_mods(&m.path, content))
}

fn emit_client_row(row: &Row) -> Result<TokenStream, CodegenError> {
    let name = format_ident!("{}", row.name);
    let pk_name = format_ident!("{}", row.pk_name);
    let pk_ty = &row.pk_ty;
    let field_defs = row.fields.iter().map(|(n, t)| {
        let ident = format_ident!("{n}");
        quote! { pub #ident: #t }
    });
    let dbtable_impl = emit_dbtable_impl(&row.name, &row.fields, &row.pk_name)?;

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
) -> Result<TokenStream, CodegenError> {
    let name = format_ident!("{}", struct_name);
    let table_lit = pascal_to_snake(struct_name);

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
    let arg_bindings = emit_arg_bindings(&q.fn_name, &q.params)?;

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

        // `tables_client::fetch` wraps `wasm_bindgen_futures::JsFuture`,
        // whose inner state (`Rc<RefCell<_>>`) is `!Send`. On native, the
        // native `FetcherFuture` carries `+ Send`, so this impl only
        // type-checks under wasm32. The corresponding `FetcherFuture` /
        // `AsyncFetcherFn` aliases in sql-engine drop the `Send` bound
        // there to match.
        #[cfg(target_arch = "wasm32")]
        impl ::sql_engine::DbCaller for #marker {
            const ID: &'static str = #id_lit;
            type Ctx = ();
            type Row = #row_ty;

            fn meta() -> ::sql_engine::planner::requirement::RequirementMeta {
                ::sql_engine::planner::requirement::RequirementMeta {
                    row_table: <<Self as ::sql_engine::DbCaller>::Row as ::sql_engine::DbTable>::TABLE.into(),
                    params: ::std::vec![ #(#param_defs),* ],
                }
            }

            fn call(
                args: ::std::vec::Vec<::sql_parser::ast::Value>,
                _ctx: ::std::sync::Arc<Self::Ctx>,
            ) -> ::sql_engine::execute::FetcherFuture {
                ::std::boxed::Box::pin(async move {
                    #(#arg_bindings)*
                    let params = #marker { #(#param_names),* };
                    let rows = ::tables_client::fetch::<#marker>(#url, params).await
                        .map_err(|e| ::std::string::ToString::to_string(&e))?;
                    Ok(rows.into_iter()
                        .map(<#row_ty as ::sql_engine::DbTable>::into_cells)
                        .collect())
                })
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
    let arg_bindings = emit_arg_bindings(&q.fn_name, &q.params)?;
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

        impl ::sql_engine::DbCaller for #marker {
            const ID: &'static str = #id_lit;
            type Ctx = #ctx_ty;
            type Row = #row_path;

            fn meta() -> ::sql_engine::planner::requirement::RequirementMeta {
                ::sql_engine::planner::requirement::RequirementMeta {
                    row_table: <<Self as ::sql_engine::DbCaller>::Row as ::sql_engine::DbTable>::TABLE.into(),
                    params: ::std::vec![ #(#param_defs),* ],
                }
            }

            fn call(
                args: ::std::vec::Vec<::sql_parser::ast::Value>,
                ctx: ::std::sync::Arc<Self::Ctx>,
            ) -> ::sql_engine::execute::FetcherFuture {
                ::std::boxed::Box::pin(async move {
                    #(#arg_bindings)*
                    let rows = #user_mod::#fn_name(#(#param_idents,)* &*ctx).await
                        .map_err(|e| ::std::string::ToString::to_string(&e))?;
                    Ok(rows.into_iter()
                        .map(<#row_path as ::sql_engine::DbTable>::into_cells)
                        .collect())
                })
            }
        }

        pub fn #register_ident(registry: &mut ::tables_storage::Registry<#ctx_ty>) {
            registry.register::<#marker>(|params, ctx| {
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
                ::sql_engine::planner::requirement::RequirementParamDef {
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
            let err_non_null_int = format!("{err_prefix}expected Int, got NULL");
            let err_non_null_text = format!("{err_prefix}expected Text, got NULL");

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
            };
            Ok(binding)
        })
        .collect()
}

fn wrap_in_mods(path: &[String], content: TokenStream) -> TokenStream {
    let mut ts = content;
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

/// Path to the user's module (where the query fn + row struct live),
/// absolute from the crate root.
fn user_mod_path(path: &[String]) -> TokenStream {
    let idents: Vec<_> = path.iter().map(|s| format_ident!("{s}")).collect();
    quote! { crate::#(#idents)::* }
}
