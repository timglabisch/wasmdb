//! Emit generated code from a parsed [`Model`].
//!
//! Both modes produce one top-level `TokenStream` suitable for
//! `include!`-ing inside `mod __generated { ... }` in the caller crate.
//! Paths into user code use `crate::<mod_path>::...` absolute references.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::Type;

use crate::parse::{Model, Module, Query, Row};
use crate::CodegenError;

pub fn emit_client(model: &Model, url: &str, wasm_bindings: bool) -> TokenStream {
    let modules = model
        .modules
        .iter()
        .map(|m| emit_client_module(m, url));
    let wasm = if wasm_bindings {
        let bindings = model
            .modules
            .iter()
            .flat_map(|m| m.queries.iter().map(move |q| emit_wasm_binding(m, q, url)));
        quote! { #(#bindings)* }
    } else {
        quote! {}
    };
    quote! {
        #(#modules)*
        #wasm
    }
}

pub fn emit_server(model: &Model, ctx_ty_str: &str) -> Result<TokenStream, CodegenError> {
    let ctx_ty: Type = syn::parse_str(ctx_ty_str)
        .map_err(|e| CodegenError::Emit(format!("ctx_type `{ctx_ty_str}`: {e}")))?;

    let modules = model
        .modules
        .iter()
        .map(|m| emit_server_module(m, &ctx_ty))
        .collect::<Vec<_>>();

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

fn emit_client_module(m: &Module, url: &str) -> TokenStream {
    let rows = m.rows.iter().map(emit_client_row);
    let queries = m.queries.iter().map(|q| emit_client_query(q, url));
    let content = quote! {
        #(#rows)*
        #(#queries)*
    };
    wrap_in_mods(&m.path, content)
}

fn emit_server_module(m: &Module, ctx_ty: &Type) -> TokenStream {
    let user_mod = user_mod_path(&m.path);
    let queries = m.queries.iter().map(|q| emit_server_query(q, ctx_ty, &user_mod));
    let content = quote! {
        #(#queries)*
    };
    wrap_in_mods(&m.path, content)
}

fn emit_client_row(row: &Row) -> TokenStream {
    let name = format_ident!("{}", row.name);
    let pk_name = format_ident!("{}", row.pk_name);
    let pk_ty = &row.pk_ty;
    let fields = row.fields.iter().map(|(n, t)| {
        let ident = format_ident!("{n}");
        quote! { pub #ident: #t }
    });

    quote! {
        #[derive(
            ::core::fmt::Debug,
            ::core::clone::Clone,
            ::borsh::BorshSerialize,
            ::borsh::BorshDeserialize,
            ::serde::Serialize,
            ::serde::Deserialize,
        )]
        pub struct #name {
            #(#fields,)*
        }

        impl ::tables::Row for #name {
            type Pk = #pk_ty;
            fn pk(&self) -> Self::Pk {
                ::core::clone::Clone::clone(&self.#pk_name)
            }
        }
    }
}

fn emit_client_query(q: &Query, url: &str) -> TokenStream {
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

    quote! {
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
    }
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

fn emit_server_query(q: &Query, ctx_ty: &Type, user_mod: &TokenStream) -> TokenStream {
    let fn_name = format_ident!("{}", q.fn_name);
    let marker = format_ident!("{}", q.marker_name);
    let id_lit = &q.id;
    let register_ident = format_ident!("register_{}", q.fn_name);
    let row_ty = &q.row_ty;

    let param_fields = q.params.iter().map(|(n, t)| {
        let ident = format_ident!("{n}");
        quote! { pub #ident: #t }
    });
    let param_pushes: Vec<_> = q.params.iter().map(|(n, _)| {
        let ident = format_ident!("{n}");
        quote! { params.#ident }
    }).collect();

    quote! {
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
            type Row = #user_mod::#row_ty;
        }

        pub fn #register_ident(registry: &mut ::tables_storage::Registry<#ctx_ty>) {
            registry.register::<#marker>(|params, ctx| {
                ::std::boxed::Box::pin(async move {
                    #user_mod::#fn_name(#(#param_pushes,)* ctx).await
                        .map_err(|e| ::tables_storage::StorageError::Storage(
                            ::std::string::ToString::to_string(&e),
                        ))
                })
            });
        }
    }
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
