use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{Expr, Pat, Token};

/// `sql_batch!(<pat> = <subject> => [stmt, stmt, ...])`
///
/// Expands to a block that destructures `<subject>` against `<pat>` (so the
/// pattern's bindings are in scope for the inner expressions) and returns
/// the array of statements.
struct BatchInput {
    pattern: Pat,
    subject: Expr,
    items: Vec<Expr>,
}

impl Parse for BatchInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let pattern = Pat::parse_single(input)?;
        input.parse::<Token![=]>()?;
        let subject: Expr = input.parse()?;
        input.parse::<Token![=>]>()?;

        let content;
        syn::bracketed!(content in input);
        let items: syn::punctuated::Punctuated<Expr, Token![,]> =
            content.parse_terminated(Expr::parse, Token![,])?;
        let items = items.into_iter().collect();

        Ok(BatchInput { pattern, subject, items })
    }
}

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let BatchInput { pattern, subject, items } = syn::parse2(input)?;
    Ok(quote! {
        {
            let #pattern = #subject;
            [ #( #items ),* ]
        }
    })
}
