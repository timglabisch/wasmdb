//! Proc-macros for the `tables` ecosystem.
//!
//! - `#[row]` on a struct: emits derives and `impl Row`. Needs one
//!   `#[pk]` field.
//!
//! - `#[query]` (optional `id = "..."`) on an async fn: **near-no-op**.
//!   Validates signature (`async`, last arg `&T`, return
//!   `Result<Vec<_>, _>`), then returns the fn unchanged — except that
//!   an inherited visibility is promoted to `pub(crate)` so generated
//!   glue (in a sibling `__generated` module) can call it via
//!   absolute path. If `id` is omitted, the codegen defaults to
//!   `<module::path>::<fn_name>`.
//!
//!   The actual wire machinery (`Params` struct, `impl Fetcher`,
//!   `register_{fn}`) is emitted by `tables-codegen` in the crate's
//!   `build.rs`. Both server and client build from the same source,
//!   eliminating wire drift.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{
    parse_macro_input, Data, DeriveInput, Error, Fields, FnArg, Ident, ItemFn, LitStr, PatType,
    ReturnType, Token, Type, TypePath, Visibility,
};

// ============================================================
// #[row]
// ============================================================

#[proc_macro_attribute]
pub fn row(args: TokenStream, input: TokenStream) -> TokenStream {
    if !args.is_empty() {
        let args2: TokenStream2 = args.into();
        return Error::new_spanned(args2, "#[row] takes no arguments — use #[pk] on a field")
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
        let Some(ident) = field.ident.clone() else {
            continue;
        };
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
// #[query]
// ============================================================

struct QueryArgs {
    _id: Option<String>,
}

impl Parse for QueryArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        if input.is_empty() {
            return Ok(Self { _id: None });
        }
        let ident: Ident = input.parse()?;
        if ident != "id" {
            return Err(syn::Error::new(ident.span(), "expected `id = \"...\"`"));
        }
        let _: Token![=] = input.parse()?;
        let lit: LitStr = input.parse()?;
        Ok(Self { _id: Some(lit.value()) })
    }
}

#[proc_macro_attribute]
pub fn query(args: TokenStream, input: TokenStream) -> TokenStream {
    let _args = parse_macro_input!(args as QueryArgs);
    let input = parse_macro_input!(input as ItemFn);

    match expand_query(input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn expand_query(mut input: ItemFn) -> syn::Result<TokenStream2> {
    if input.sig.asyncness.is_none() {
        return Err(Error::new_spanned(&input.sig, "#[query] requires `async fn`"));
    }

    let args: Vec<&FnArg> = input.sig.inputs.iter().collect();
    if args.len() < 2 {
        return Err(Error::new_spanned(
            &input.sig,
            "#[query] needs at least one param before the ctx arg",
        ));
    }

    // Last arg must be `&T` (ctx).
    let ctx_arg = args.last().unwrap();
    let FnArg::Typed(PatType { ty: ctx_ty, .. }) = ctx_arg else {
        return Err(Error::new_spanned(
            ctx_arg,
            "#[query] ctx arg must not be `self`",
        ));
    };
    if !matches!(ctx_ty.as_ref(), Type::Reference(_)) {
        return Err(Error::new_spanned(
            ctx_ty,
            "#[query] ctx arg must be a shared reference like `&AppCtx`",
        ));
    }

    // Return must be `Result<Vec<_>, _>`.
    let ReturnType::Type(_, ret) = &input.sig.output else {
        return Err(Error::new_spanned(
            &input.sig,
            "#[query] return must be `Result<Vec<Row>, _>`",
        ));
    };
    if !is_result_vec(ret) {
        return Err(Error::new_spanned(
            ret.as_ref(),
            "#[query] return must be `Result<Vec<Row>, _>`",
        ));
    }

    // Promote inherited visibility so generated glue in a sibling
    // `__generated` module can call the fn via `crate::path::fn`.
    if matches!(input.vis, Visibility::Inherited) {
        input.vis = syn::parse_quote!(pub(crate));
    }

    Ok(quote! { #input })
}

fn is_result_vec(ty: &Type) -> bool {
    let Type::Path(TypePath { path, .. }) = ty else {
        return false;
    };
    let Some(last) = path.segments.last() else {
        return false;
    };
    if last.ident != "Result" {
        return false;
    }
    let syn::PathArguments::AngleBracketed(args) = &last.arguments else {
        return false;
    };
    let Some(syn::GenericArgument::Type(ok)) = args.args.first() else {
        return false;
    };
    let Type::Path(TypePath { path: vp, .. }) = ok else {
        return false;
    };
    vp.segments.last().map(|s| s.ident == "Vec").unwrap_or(false)
}
