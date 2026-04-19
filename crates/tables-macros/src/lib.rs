//! Proc-macros for the `tables` ecosystem.
//!
//! - `#[row]` on a struct: emits derives and `impl Row`. Needs one
//!   `#[pk]` field.
//!
//! - `#[fetcher(row = RowType)]` on a struct: the struct itself _is_
//!   the params. Emits derives and `impl Fetcher` binding the user
//!   struct to a row type. The wire id is auto-derived from the module
//!   path + struct name.
//!
//! - `#[storage]` on an async fn: emits `pub fn register_{fn}` that
//!   wires the fn into a `Registry`. The fetcher marker is read from
//!   the fn's first argument type. Handles `Box::pin` and error mapping
//!   (any `Display` error → `StorageError::Storage`).

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{
    parse_macro_input, Data, DeriveInput, Error, Expr, Fields, FnArg, ItemFn, MetaNameValue, Pat,
    PatType, Path, Token, Type, TypePath, TypeReference, Visibility,
};

// ============================================================
// #[row]
// ============================================================

#[proc_macro_attribute]
pub fn row(args: TokenStream, input: TokenStream) -> TokenStream {
    if !args.is_empty() {
        let args2: TokenStream2 = args.into();
        return Error::new_spanned(
            args2,
            "#[row] takes no arguments — use #[pk] on a field",
        )
        .to_compile_error()
        .into();
    }
    let input = parse_macro_input!(input as DeriveInput);
    match expand_row(input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn expand_row(mut input: DeriveInput) -> syn::Result<TokenStream2> {
    let name = input.ident.clone();

    let Data::Struct(ds) = &mut input.data else {
        return Err(Error::new_spanned(&input, "#[row] only works on structs"));
    };
    let Fields::Named(named) = &mut ds.fields else {
        return Err(Error::new_spanned(&input, "#[row] needs named fields"));
    };

    let mut pk_field: Option<(syn::Ident, Type)> = None;

    for field in named.named.iter_mut() {
        let Some(ident) = field.ident.clone() else { continue };
        let mut is_pk = false;
        field.attrs.retain(|attr| {
            if attr.path().is_ident("pk") {
                is_pk = true;
                false
            } else {
                true
            }
        });
        if is_pk {
            if pk_field.is_some() {
                return Err(Error::new_spanned(field, "more than one #[pk] field"));
            }
            pk_field = Some((ident, field.ty.clone()));
        }
    }

    let (pk_name, pk_ty) = pk_field.ok_or_else(|| {
        Error::new_spanned(&input, "missing #[pk] field — exactly one required")
    })?;

    Ok(quote! {
        #[derive(
            ::core::fmt::Debug,
            ::core::clone::Clone,
            ::borsh::BorshSerialize,
            ::borsh::BorshDeserialize,
            ::serde::Serialize,
            ::serde::Deserialize,
        )]
        #input

        impl ::tables::Row for #name {
            type Pk = #pk_ty;
            fn pk(&self) -> #pk_ty {
                ::core::clone::Clone::clone(&self.#pk_name)
            }
        }
    })
}

// ============================================================
// #[fetcher]
// ============================================================

struct FetcherArgs {
    row: Path,
}

impl Parse for FetcherArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let metas: Punctuated<MetaNameValue, Token![,]> =
            Punctuated::parse_terminated(input)?;
        let mut row = None;
        for meta in metas {
            if meta.path.is_ident("row") {
                if let Expr::Path(p) = meta.value {
                    row = Some(p.path);
                } else {
                    return Err(Error::new_spanned(meta.value, "row must be a type path"));
                }
            } else {
                return Err(Error::new_spanned(meta.path, "unknown attribute key"));
            }
        }
        let row = row.ok_or_else(|| Error::new(input.span(), "missing required `row = ...`"))?;
        Ok(Self { row })
    }
}

#[proc_macro_attribute]
pub fn fetcher(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as FetcherArgs);
    let input = parse_macro_input!(input as DeriveInput);
    match expand_fetcher(args, input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn expand_fetcher(args: FetcherArgs, input: DeriveInput) -> syn::Result<TokenStream2> {
    let name = input.ident.clone();
    let name_lit = name.to_string();
    let row_ty = &args.row;

    // Must be a struct (possibly empty) — the struct _is_ the params.
    if !matches!(&input.data, Data::Struct(_)) {
        return Err(Error::new_spanned(&input, "#[fetcher] only works on structs"));
    }

    Ok(quote! {
        #[derive(
            ::core::fmt::Debug,
            ::core::clone::Clone,
            ::borsh::BorshSerialize,
            ::borsh::BorshDeserialize,
            ::serde::Serialize,
            ::serde::Deserialize,
        )]
        #input

        impl ::tables::Fetcher for #name {
            const ID: ::tables::FetcherId = ::core::concat!(
                ::core::module_path!(),
                "::",
                #name_lit,
            );
            type Params = #name;
            type Row = #row_ty;
        }
    })
}

// ============================================================
// #[storage]
// ============================================================

#[proc_macro_attribute]
pub fn storage(args: TokenStream, input: TokenStream) -> TokenStream {
    if !args.is_empty() {
        let args2: TokenStream2 = args.into();
        return Error::new_spanned(
            args2,
            "#[storage] takes no arguments — the fetcher marker is read from the first fn parameter",
        )
        .to_compile_error()
        .into();
    }
    let input = parse_macro_input!(input as ItemFn);

    match expand_storage(input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn expand_storage(input: ItemFn) -> syn::Result<TokenStream2> {
    if input.sig.asyncness.is_none() {
        return Err(Error::new_spanned(&input.sig, "#[storage] requires `async fn`"));
    }
    let fn_name = &input.sig.ident;
    let register_ident = quote::format_ident!("register_{}", fn_name);

    let mut inputs = input.sig.inputs.iter();
    let params_arg = inputs.next().ok_or_else(|| {
        Error::new_spanned(&input.sig, "fn needs (params, ctx) — params arg missing")
    })?;
    let ctx_arg = inputs
        .next()
        .ok_or_else(|| Error::new_spanned(&input.sig, "fn needs (params, ctx) — ctx arg missing"))?;

    let fetcher = extract_path_type(params_arg)?;
    let ctx_ty = extract_ref_type(ctx_arg)?;

    let vis = match &input.vis {
        Visibility::Inherited => quote!(pub),
        v => quote!(#v),
    };

    Ok(quote! {
        #input

        #vis fn #register_ident(registry: &mut ::tables_storage::Registry<#ctx_ty>) {
            registry.register::<#fetcher>(|params, ctx| {
                ::std::boxed::Box::pin(async move {
                    #fn_name(params, ctx).await.map_err(|e| {
                        ::tables_storage::StorageError::Storage(
                            ::std::string::ToString::to_string(&e),
                        )
                    })
                })
            });
        }
    })
}

fn extract_path_type(arg: &FnArg) -> syn::Result<Path> {
    let FnArg::Typed(PatType { ty, .. }) = arg else {
        return Err(Error::new_spanned(arg, "params arg must not be `self`"));
    };
    let Type::Path(TypePath { qself: None, path }) = ty.as_ref() else {
        return Err(Error::new_spanned(
            ty,
            "params arg type must be a plain type path (the fetcher marker)",
        ));
    };
    Ok(path.clone())
}

fn extract_ref_type(arg: &FnArg) -> syn::Result<Type> {
    let FnArg::Typed(PatType { ty, pat, .. }) = arg else {
        return Err(Error::new_spanned(arg, "ctx arg must not be `self`"));
    };
    let Type::Reference(TypeReference { elem, mutability: None, .. }) = ty.as_ref() else {
        return Err(Error::new_spanned(
            ty,
            "ctx arg must be a shared reference like `&AppCtx`",
        ));
    };
    if !matches!(pat.as_ref(), Pat::Ident(_)) {
        return Err(Error::new_spanned(pat, "ctx arg needs a simple binding"));
    }
    Ok((**elem).clone())
}
