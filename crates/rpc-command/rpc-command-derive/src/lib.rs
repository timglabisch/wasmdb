//! Proc-macros for `#[rpc_command]` (per struct) and `#[rpc_command_enum]`
//! (per wire-format enum). Per-variant `#[rpc_command_skip]` excludes a
//! variant from the bundle.
//!
//! The struct macro emits:
//!   * the standard derive bundle
//!   * an inherent `pub const __RPC_COMMAND_SPEC: rpc_command::StructSpec<'static>`
//!
//! The enum macro emits a `#[cfg(test)] #[test] fn` that gathers each
//! non-skipped variant's spec and calls `rpc_command::export_bundle`.

use proc_macro::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{parse_macro_input, Expr, Fields, ItemEnum, ItemStruct, Lit, LitStr, Meta, Token};

// ── #[rpc_command] ────────────────────────────────────────────────────────

#[proc_macro_attribute]
pub fn rpc_command(attr: TokenStream, item: TokenStream) -> TokenStream {
    if !attr.is_empty() {
        return syn::Error::new_spanned(
            proc_macro2::TokenStream::from(attr),
            "rpc_command takes no arguments — config lives on the enum",
        )
        .to_compile_error()
        .into();
    }
    let mut s = parse_macro_input!(item as ItemStruct);
    let struct_name = s.ident.clone();
    let struct_name_str = struct_name.to_string();
    let camel_fn = camel_case(&struct_name_str);

    let mut input_fields: Vec<String> = Vec::new();
    let mut auto_fields: Vec<(String, String)> = Vec::new();

    for f in s.fields.iter_mut() {
        let name = match &f.ident {
            Some(i) => i.to_string(),
            None => {
                return syn::Error::new_spanned(
                    f,
                    "rpc_command: tuple structs are not supported — use a named-field struct",
                )
                .to_compile_error()
                .into();
            }
        };
        let mut default_expr: Option<String> = None;
        let mut err: Option<syn::Error> = None;
        f.attrs.retain(|a| {
            if a.path().is_ident("client_default") {
                if let Meta::NameValue(nv) = &a.meta {
                    if let Expr::Lit(syn::ExprLit { lit: Lit::Str(s), .. }) = &nv.value {
                        default_expr = Some(s.value());
                        return false;
                    }
                }
                err = Some(syn::Error::new_spanned(
                    a,
                    "client_default must be `#[client_default = \"<ts expr>\"]`",
                ));
            }
            true
        });
        if let Some(e) = err {
            return e.to_compile_error().into();
        }
        match default_expr {
            Some(e) => auto_fields.push((name, e)),
            None => input_fields.push(name),
        }
    }

    let camel_lit = LitStr::new(&camel_fn, proc_macro2::Span::call_site());
    let struct_name_lit = LitStr::new(&struct_name_str, proc_macro2::Span::call_site());
    let input_lits: Vec<_> = input_fields
        .iter()
        .map(|s| LitStr::new(s, proc_macro2::Span::call_site()))
        .collect();
    let auto_name_lits: Vec<_> = auto_fields
        .iter()
        .map(|(n, _)| LitStr::new(n, proc_macro2::Span::call_site()))
        .collect();
    let auto_expr_lits: Vec<_> = auto_fields
        .iter()
        .map(|(_, e)| LitStr::new(e, proc_macro2::Span::call_site()))
        .collect();

    let export_path_lit = ts_export_path_lit();

    let expanded = quote! {
        #[derive(
            ::std::fmt::Debug,
            ::std::clone::Clone,
            ::borsh::BorshSerialize,
            ::borsh::BorshDeserialize,
            ::serde::Serialize,
            ::serde::Deserialize,
            ::ts_rs::TS,
        )]
        #[ts(export_to = #export_path_lit)]
        #s

        impl #struct_name {
            #[allow(non_upper_case_globals)]
            pub const __RPC_COMMAND_SPEC: ::rpc_command::StructSpec<'static> =
                ::rpc_command::StructSpec {
                    struct_name: #struct_name_lit,
                    fn_name: #camel_lit,
                    inputs: &[#(#input_lits),*],
                    auto: &[#( (#auto_name_lits, #auto_expr_lits) ),*],
                };
        }
    };
    expanded.into()
}

// ── #[rpc_command_enum] ───────────────────────────────────────────────────

struct EnumArgs {
    /// Optional override of the bundle filename (sans `.ts`).
    /// Defaults to `<EnumName>Factories`.
    bundle_name: Option<String>,
}

impl Parse for EnumArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        if input.is_empty() {
            return Ok(EnumArgs { bundle_name: None });
        }
        let mut bundle_name: Option<String> = None;
        let pairs = Punctuated::<syn::MetaNameValue, Token![,]>::parse_terminated(input)?;
        for nv in pairs {
            let key = nv
                .path
                .get_ident()
                .map(|i| i.to_string())
                .unwrap_or_default();
            match key.as_str() {
                "bundle_name" => {
                    bundle_name = Some(expect_str(&nv.value)?);
                }
                other => {
                    return Err(syn::Error::new_spanned(
                        &nv.path,
                        format!("rpc_command_enum: unknown key `{other}`"),
                    ));
                }
            }
        }
        Ok(EnumArgs { bundle_name })
    }
}

fn expect_str(e: &Expr) -> syn::Result<String> {
    match e {
        Expr::Lit(lit) => match &lit.lit {
            Lit::Str(s) => Ok(s.value()),
            _ => Err(syn::Error::new_spanned(e, "expected string literal")),
        },
        _ => Err(syn::Error::new_spanned(e, "expected literal")),
    }
}

#[proc_macro_attribute]
pub fn rpc_command_enum(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as EnumArgs);
    let mut e = parse_macro_input!(item as ItemEnum);
    let enum_name = e.ident.clone();
    let enum_name_str = enum_name.to_string();
    let bundle_name = args.bundle_name.unwrap_or_else(|| format!("{enum_name_str}Factories"));

    // Walk variants — pick those that wrap a single command struct and are NOT
    // marked `#[rpc_command_skip]`.
    let mut variant_types: Vec<syn::Path> = Vec::new();
    for v in e.variants.iter_mut() {
        let mut skip = false;
        v.attrs.retain(|a| {
            if a.path().is_ident("rpc_command_skip") {
                skip = true;
                return false;
            }
            true
        });
        if skip {
            continue;
        }
        match &v.fields {
            Fields::Unnamed(unnamed) if unnamed.unnamed.len() == 1 => {
                let ty = &unnamed.unnamed[0].ty;
                match ty {
                    syn::Type::Path(tp) => variant_types.push(tp.path.clone()),
                    _ => {
                        return syn::Error::new_spanned(
                            ty,
                            "rpc_command_enum: variant payload must be a path type \
                             (a command struct). Use #[rpc_command_skip] to exclude.",
                        )
                        .to_compile_error()
                        .into();
                    }
                }
            }
            Fields::Unit => {
                return syn::Error::new_spanned(
                    v,
                    "rpc_command_enum: unit variants are not supported. \
                     Use #[rpc_command_skip] to exclude.",
                )
                .to_compile_error()
                .into();
            }
            _ => {
                return syn::Error::new_spanned(
                    v,
                    "rpc_command_enum: only single-payload tuple variants are supported. \
                     Use #[rpc_command_skip] to exclude.",
                )
                .to_compile_error()
                .into();
            }
        }
    }

    let bundle_lit = LitStr::new(&bundle_name, proc_macro2::Span::call_site());
    let test_fn = syn::Ident::new(
        &format!("__rpc_command_bundle_{enum_name_str}"),
        proc_macro2::Span::call_site(),
    );
    let export_path_lit = ts_export_path_lit();
    // Append AFTER existing attrs so the user-supplied `#[derive(TS)]` (which
    // introduces the `ts` helper) is in scope when the compiler sees our
    // injected `#[ts(export_to = ...)]`.
    e.attrs
        .push(syn::parse_quote!(#[ts(export_to = #export_path_lit)]));

    let expanded = quote! {
        #e

        #[cfg(test)]
        #[test]
        #[allow(non_snake_case)]
        fn #test_fn() {
            let specs: &[::rpc_command::StructSpec<'static>] = &[
                #( <#variant_types>::__RPC_COMMAND_SPEC ),*
            ];
            ::rpc_command::export_bundle_named(#bundle_lit, specs, stringify!(#enum_name));
        }
    };
    expanded.into()
}

/// Convention: per-command + enum bindings land next to the
/// `<EnumName>Factories.ts` bundle. Honors `RPC_COMMAND_TS_ROOT` (typically
/// set via `cargo:rustc-env=` from the consuming crate's build script);
/// otherwise falls back to `<CARGO_MANIFEST_DIR>/../../frontend/ui/src/generated/`.
/// Mirrors `rpc_command::ts_root()`. Path is computed at proc-macro expansion
/// time and baked into each `#[ts(export_to = ...)]` attribute.
fn ts_export_path_lit() -> LitStr {
    let path = if let Ok(v) = std::env::var("RPC_COMMAND_TS_ROOT") {
        if v.ends_with('/') { v } else { format!("{v}/") }
    } else {
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
            .expect("CARGO_MANIFEST_DIR — proc-macro runs under cargo");
        format!("{manifest_dir}/../../frontend/ui/src/generated/")
    };
    LitStr::new(&path, proc_macro2::Span::call_site())
}

fn camel_case(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        None => String::new(),
        Some(first) => {
            let mut out: String = first.to_lowercase().collect();
            out.push_str(chars.as_str());
            out
        }
    }
}
