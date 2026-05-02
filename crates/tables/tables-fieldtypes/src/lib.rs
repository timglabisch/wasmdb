//! Rust-type → `DataType` classification, shared between `#[row]` /
//! `#[query]` (proc-macro) and `tables-codegen` (build-time).
//!
//! The supported shapes — `i64`, `String`, `Option<i64>`, `Option<String>`
//! — mirror `sql_engine::schema::DataType` plus nullability. Keep this
//! surface narrow: every addition here has to land on both consumers,
//! so prune unsupported types at the edge rather than letting them leak
//! into the SQL layer.

use proc_macro2::TokenStream;
use quote::quote;
use syn::Type;

#[derive(Clone, Copy, Debug)]
pub enum FieldKind {
    I64,
    Str,
    Uuid,
    OptI64,
    OptStr,
    OptUuid,
}

impl FieldKind {
    /// `(DataType tokens, nullable)` for building `ColumnSchema` or
    /// `RequirementParamDef` entries. Tokens reference
    /// `::sql_engine::schema::DataType` by absolute path so callers
    /// don't need any particular import in scope.
    pub fn column_meta(self) -> (TokenStream, bool) {
        match self {
            FieldKind::I64 => (quote! { ::sql_engine::schema::DataType::I64 }, false),
            FieldKind::Str => (quote! { ::sql_engine::schema::DataType::String }, false),
            FieldKind::Uuid => (quote! { ::sql_engine::schema::DataType::Uuid }, false),
            FieldKind::OptI64 => (quote! { ::sql_engine::schema::DataType::I64 }, true),
            FieldKind::OptStr => (quote! { ::sql_engine::schema::DataType::String }, true),
            FieldKind::OptUuid => (quote! { ::sql_engine::schema::DataType::Uuid }, true),
        }
    }
}

/// Match a `syn::Type` against the supported shape set. `None` means
/// the type is unsupported — callers turn that into a compile-error /
/// `CodegenError` with a type-specific message.
///
/// `Uuid` is recognized as the bare ident — callers are expected to pull
/// in a 16-byte representation (typically a tuple-struct around `[u8; 16]`)
/// and either bring it into scope with that name or `use ... as Uuid`.
pub fn classify(ty: &Type) -> Option<FieldKind> {
    if let Some(inner) = option_inner(ty) {
        return match simple_ident_name(inner)?.as_str() {
            "i64" => Some(FieldKind::OptI64),
            "String" => Some(FieldKind::OptStr),
            "Uuid" => Some(FieldKind::OptUuid),
            _ => None,
        };
    }
    match simple_ident_name(ty)?.as_str() {
        "i64" => Some(FieldKind::I64),
        "String" => Some(FieldKind::Str),
        "Uuid" => Some(FieldKind::Uuid),
        _ => None,
    }
}

fn option_inner(ty: &Type) -> Option<&Type> {
    let Type::Path(syn::TypePath { path, .. }) = ty else {
        return None;
    };
    let seg = path.segments.last()?;
    if seg.ident != "Option" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &seg.arguments else {
        return None;
    };
    args.args.iter().find_map(|a| match a {
        syn::GenericArgument::Type(t) => Some(t),
        _ => None,
    })
}

fn simple_ident_name(ty: &Type) -> Option<String> {
    let Type::Path(syn::TypePath { path, .. }) = ty else {
        return None;
    };
    let seg = path.segments.last()?;
    if !seg.arguments.is_empty() {
        return None;
    }
    Some(seg.ident.to_string())
}

/// `Customer` → `"customer"`, `InvoiceItem` → `"invoice_item"`.
pub fn pascal_to_snake(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    for (i, c) in s.chars().enumerate() {
        if c.is_ascii_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}
