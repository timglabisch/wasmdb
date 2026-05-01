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
use tables_fieldtypes::{classify, pascal_to_snake, FieldKind};

// ============================================================
// #[row]
// ============================================================

#[proc_macro_attribute]
pub fn row(args: TokenStream, input: TokenStream) -> TokenStream {
    let row_args = parse_macro_input!(args as RowArgs);
    let input = parse_macro_input!(input as DeriveInput);
    match expand_row(input, row_args) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

struct RowArgs {
    table: Option<String>,
}

impl Parse for RowArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        if input.is_empty() {
            return Ok(Self { table: None });
        }
        let ident: Ident = input.parse()?;
        if ident != "table" {
            return Err(syn::Error::new(ident.span(), "expected `table = \"...\"`"));
        }
        let _eq: Token![=] = input.parse()?;
        let lit: LitStr = input.parse()?;
        Ok(Self { table: Some(lit.value()) })
    }
}

fn expand_row(mut input: DeriveInput, args: RowArgs) -> syn::Result<TokenStream2> {
    let name = input.ident.clone();

    // Strip our struct-level marker attributes that codegen reads from the
    // source file but the compiler should not see (no registered handler).
    input.attrs.retain(|attr| !attr.path().is_ident("export"));

    let Data::Struct(ds) = &mut input.data else {
        return Err(Error::new_spanned(&input, "#[row] only works on structs"));
    };
    let Fields::Named(named) = &mut ds.fields else {
        return Err(Error::new_spanned(&input, "#[row] needs named fields"));
    };

    struct RowField {
        ident: syn::Ident,
        ty: Type,
        kind: FieldKind,
        is_pk: bool,
    }

    let mut fields: Vec<RowField> = Vec::new();
    let mut pk_seen = false;

    for field in named.named.iter_mut() {
        let Some(ident) = field.ident.clone() else {
            continue;
        };
        let mut is_pk = false;
        field.attrs.retain(|attr| {
            if attr.path().is_ident("pk") {
                is_pk = true;
                false
            } else if attr.path().is_ident("group") {
                // Field-level group tag; codegen scans the source file for it.
                false
            } else {
                true
            }
        });
        if is_pk {
            if pk_seen {
                return Err(Error::new_spanned(field, "more than one #[pk] field"));
            }
            pk_seen = true;
        }
        let kind = classify(&field.ty).ok_or_else(|| {
            Error::new_spanned(
                &field.ty,
                format!(
                    "#[row] field `{ident}`: supported types are i64, String, Option<i64>, Option<String>",
                ),
            )
        })?;
        fields.push(RowField { ident, ty: field.ty.clone(), kind, is_pk });
    }

    let pk_idx = fields
        .iter()
        .position(|f| f.is_pk)
        .ok_or_else(|| Error::new_spanned(&input, "missing #[pk] field — exactly one required"))?;
    let pk_name = fields[pk_idx].ident.clone();
    let pk_ty = fields[pk_idx].ty.clone();

    let table_lit = args
        .table
        .unwrap_or_else(|| pascal_to_snake(&name.to_string()));

    let column_defs = fields.iter().map(|f| {
        let col_name = f.ident.to_string();
        let (dt, nullable) = f.kind.column_meta();
        quote! {
            ::sql_engine::schema::ColumnSchema {
                name: #col_name.into(),
                data_type: #dt,
                nullable: #nullable,
            }
        }
    });

    let cell_pushes = fields.iter().map(|f| {
        let ident = &f.ident;
        match f.kind {
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
        }
    });

    let n_fields = fields.len();

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
    if args.is_empty() {
        return Err(Error::new_spanned(
            &input.sig,
            "#[query] needs a ctx arg",
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
