//! `#[derive(FromRow)]`: positionally maps a row's `CellValue`s to a
//! struct's fields in declared order.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields};

pub fn expand(input: DeriveInput) -> syn::Result<TokenStream> {
    let name = &input.ident;

    let fields = match &input.data {
        Data::Struct(s) => match &s.fields {
            Fields::Named(named) => &named.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    name,
                    "FromRow only supports structs with named fields",
                ))
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                name,
                "FromRow only supports structs",
            ))
        }
    };

    let n_cols = fields.len();
    let assigns = fields.iter().map(|f| {
        let ident = f.ident.as_ref().unwrap();
        let ty = &f.ty;
        quote! {
            #ident: <#ty as ::sqlbuilder::FromCell>::from_cell(
                __it.next().unwrap()
            )?,
        }
    });

    Ok(quote! {
        #[automatically_derived]
        impl ::sqlbuilder::FromRow for #name {
            const COLS: usize = #n_cols;
            fn from_row(
                __row: ::std::vec::Vec<::sql_engine::storage::CellValue>,
            ) -> ::std::result::Result<Self, ::sync::command::CommandError> {
                if __row.len() != #n_cols {
                    return ::std::result::Result::Err(
                        ::sync::command::CommandError::ExecutionFailed(
                            ::std::format!(
                                "FromRow {}: expected {} columns, got {}",
                                ::std::stringify!(#name),
                                #n_cols,
                                __row.len(),
                            ),
                        ),
                    );
                }
                let mut __it = __row.into_iter();
                ::std::result::Result::Ok(Self { #(#assigns)* })
            }
        }
    })
}
