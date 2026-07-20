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
use syn::parse::{Parse, ParseStream, Parser};
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

    let cell_reads = fields.iter().enumerate().map(|(i, f)| {
        let ident = &f.ident;
        let expected = match f.kind {
            FieldKind::I64 => "I64",
            FieldKind::Str => "Str",
            FieldKind::Uuid => "Uuid",
            FieldKind::OptI64 => "I64 or Null",
            FieldKind::OptStr => "Str or Null",
            FieldKind::OptUuid => "Uuid or Null",
        };
        let err_lit = LitStr::new(
            &format!("{table_lit}.{ident}: expected {expected}, got {{:?}}"),
            proc_macro2::Span::call_site(),
        );
        let arms = match f.kind {
            FieldKind::I64 => quote! {
                ::core::option::Option::Some(::sql_engine::storage::CellValue::I64(v)) => *v,
            },
            FieldKind::Str => quote! {
                ::core::option::Option::Some(::sql_engine::storage::CellValue::Str(v)) =>
                    ::core::clone::Clone::clone(v),
            },
            FieldKind::Uuid => quote! {
                ::core::option::Option::Some(::sql_engine::storage::CellValue::Uuid(b)) =>
                    ::sql_engine::storage::Uuid(*b),
            },
            FieldKind::OptI64 => quote! {
                ::core::option::Option::Some(::sql_engine::storage::CellValue::I64(v)) =>
                    ::core::option::Option::Some(*v),
                ::core::option::Option::Some(::sql_engine::storage::CellValue::Null) =>
                    ::core::option::Option::None,
            },
            FieldKind::OptStr => quote! {
                ::core::option::Option::Some(::sql_engine::storage::CellValue::Str(v)) =>
                    ::core::option::Option::Some(::core::clone::Clone::clone(v)),
                ::core::option::Option::Some(::sql_engine::storage::CellValue::Null) =>
                    ::core::option::Option::None,
            },
            FieldKind::OptUuid => quote! {
                ::core::option::Option::Some(::sql_engine::storage::CellValue::Uuid(b)) =>
                    ::core::option::Option::Some(::sql_engine::storage::Uuid(*b)),
                ::core::option::Option::Some(::sql_engine::storage::CellValue::Null) =>
                    ::core::option::Option::None,
            },
        };
        quote! {
            let #ident = match cells.get(#i) {
                #arms
                other => return ::core::result::Result::Err(::std::format!(#err_lit, other)),
            };
        }
    });
    let field_idents = fields.iter().map(|f| &f.ident);

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

            fn from_cells(
                cells: &[::sql_engine::storage::CellValue],
            ) -> ::core::result::Result<Self, ::std::string::String> {
                #(#cell_reads)*
                ::core::result::Result::Ok(Self { #(#field_idents),* })
            }
        }
    })
}

// ============================================================
// #[projection] / #[projection_row]
// ============================================================

/// `#[projection(outputs(RowA, ...), reads(RowB, ...), id = "...")]`
/// on an inherent impl block declares a projection (design doc
/// §4.3 / §9.4). The impl target IS the fold state — a struct the
/// product declares with `#[derive(Default, Clone)]`; its fields are
/// the accumulator, one value per partition. The macro emits the user
/// impl unchanged plus an `impl database_projection::Projection` shim.
/// THE contract is the fold (§9.4): `apply` gets ONE typed log row at
/// a time — the shim feeds the partition's rows in fold order
/// (committed by seq, then pendings by provisional seq —
/// `tables::ProjectionLog::in_fold_order`) into a `Default::default()`
/// value — and `render` then emits the outputs once. Reads ONLY in
/// `render`; `apply` must stay a pure function of the rows:
///
/// ```ignore
/// #[derive(Default, Clone)]
/// pub struct DraftTotals { doc_id: i64, amount: i64 }
///
/// #[projection(outputs(DraftTotal), reads(Customer))]
/// impl DraftTotals {
///     fn apply(&mut self, row: &DraftLog) -> Result<(), String> {
///         let cmd: SetLinePrice = row.decode()?;   // decode policy is product code
///         self.amount += cmd.price_cents;
///         ...
///     }
///     fn render(
///         &self,
///         ctx: &database_projection::RenderCtx<'_>,
///         out: &mut database_projection::Out,
///     ) -> Result<(), String> { ... }
/// }
/// ```
///
/// Exactly one source: the log row type from `apply`'s parameter; the
/// partition comes from its `tables::ProjectionLog` impl (which
/// `#[projection_row]` generates). The state must be `Default + Clone`
/// (§9.4 — the snapshot below clones it). Execution (§9.3): the shim
/// memoizes the state folded over the partition's COMMITTED prefix in
/// the engine-owned `FoldCache`; a later derive resumes there and folds
/// only new committed rows plus the pendings (never memoized — they
/// reorder). A read-table change therefore re-renders without folding
/// anything. If the committed seq list stops extending the memoized one
/// (backfill, deletes, replaced data), the shim folds from zero —
/// determinism makes both paths identical in result. This relies on
/// committed log rows being immutable per (partition, seq).
///
/// `id` defaults to the snake_case struct name. The consuming crate
/// needs `database-projection`, `sql-engine` and `tables` deps.
///
/// The event LOG the projection folds is declared separately with
/// `#[projection_row]` on a struct — mirroring `#[row]` for tables.
#[proc_macro_attribute]
pub fn projection(args: TokenStream, input: TokenStream) -> TokenStream {
    let item = parse_macro_input!(input as syn::Item);
    let result = match item {
        syn::Item::Impl(imp) => match syn::parse::<ProjectionArgs>(args) {
            Ok(a) => expand_projection(imp, a),
            Err(e) => Err(e),
        },
        syn::Item::Struct(s) => Err(Error::new_spanned(
            &s.ident,
            "#[projection] goes on an inherent impl block (the projection) — \
             the event log is declared with #[projection_row] on the struct",
        )),
        other => Err(Error::new_spanned(
            &other,
            "#[projection] goes on an inherent impl block (the projection)",
        )),
    };
    match result {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// `#[projection_row]` on a struct declares a projection's event LOG —
/// the row shape behind it, mirroring `#[row]` for tables (M6a/§9.6).
/// The struct declares only `command_id` (i64 or Uuid — becomes the PK)
/// and ONE further field: the partition column, the log's stream
/// identity. The macro appends the bookkeeping columns `seq: i64`,
/// `committed: i64` and `payload: String` (the event's RPC form),
/// expands to a full `#[row]`, and implements `tables::ProjectionLog`
/// (PARTITION_COLUMN + the fold helpers
/// `decode`/`is_committed`/`in_fold_order`). Event content lives in the
/// payload, not in columns:
///
/// ```ignore
/// #[projection_row]
/// pub struct DraftLog {
///     pub command_id: i64,
///     pub doc_id: i64,     // the partition — inferred, no attribute
/// }
/// // expands to a #[row] with columns:
/// //   command_id (PK), doc_id, seq, committed, payload
/// ```
///
/// A command hand-writes `execute_optimistic` and fills exactly this
/// shape through `sync::append::{next_seq, append_row}` +
/// `rpc_command::payload_json` — appending an event is an effect the
/// command performs, not the command's identity.
#[proc_macro_attribute]
pub fn projection_row(args: TokenStream, input: TokenStream) -> TokenStream {
    let item = parse_macro_input!(input as syn::Item);
    let result = match item {
        syn::Item::Struct(s) => match check_projection_row_args(args) {
            Ok(()) => expand_projection_row(s),
            Err(e) => Err(e),
        },
        other => Err(Error::new_spanned(
            &other,
            "#[projection_row] goes on a struct (the event log declaration)",
        )),
    };
    match result {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// The log declaration takes no arguments — the partition is inferred
/// from the strict shape (§9.6). Pointed error for the retired
/// `key = ...`.
fn check_projection_row_args(args: TokenStream) -> syn::Result<()> {
    let args = TokenStream2::from(args);
    if args.is_empty() {
        return Ok(());
    }
    let msg = if args.to_string().starts_with("key") {
        "#[projection_row]: the partition is inferred from the struct shape \
         (the one field besides `command_id`) — drop `key = ...`"
    } else {
        "#[projection_row]: takes no arguments"
    };
    Err(Error::new_spanned(&args, msg))
}

/// Expand `#[projection_row]`: validate the declared shape
/// (`command_id` plus ONE partition column), mark `command_id` as PK,
/// append the generated bookkeeping columns, reuse the `#[row]`
/// expansion for derives and the `DbTable`/`Row` impls, and implement
/// `tables::ProjectionLog`.
fn expand_projection_row(mut s: syn::ItemStruct) -> syn::Result<TokenStream2> {
    let Fields::Named(named) = &mut s.fields else {
        return Err(Error::new_spanned(
            &s,
            "#[projection_row] needs named fields",
        ));
    };

    let mut has_command_id = false;
    let mut partition: Option<Ident> = None;
    for field in &named.named {
        let Some(ident) = &field.ident else { continue };
        if field.attrs.iter().any(|a| a.path().is_ident("pk")) {
            return Err(Error::new_spanned(
                field,
                "#[projection_row]: the primary key is always `command_id` — drop the #[pk]",
            ));
        }
        match ident.to_string().as_str() {
            "command_id" => has_command_id = true,
            "seq" | "committed" | "payload" => {
                return Err(Error::new_spanned(
                    field,
                    format!("#[projection_row]: `{ident}` is generated — do not declare it"),
                ));
            }
            _ => {
                if let Some(first) = &partition {
                    return Err(Error::new_spanned(
                        field,
                        format!(
                            "#[projection_row]: unexpected field `{ident}` — the log \
                             declares only `command_id` and ONE partition column \
                             (here: `{first}`); event content lives in the \
                             generated `payload` (RPC form)"
                        ),
                    ));
                }
                partition = Some(ident.clone());
            }
        }
    }
    if !has_command_id {
        return Err(Error::new_spanned(
            &s.ident,
            "#[projection_row]: needs a `command_id` field (i64 or Uuid) — it becomes the PK",
        ));
    }
    let Some(partition) = partition else {
        return Err(Error::new_spanned(
            &s.ident,
            "#[projection_row]: needs a partition column — exactly one field \
             besides `command_id` (the log's stream identity)",
        ));
    };

    for field in named.named.iter_mut() {
        if field.ident.as_ref().is_some_and(|i| i == "command_id") {
            field.attrs.push(syn::parse_quote!(#[pk]));
        }
    }
    for tokens in [
        quote! { pub seq: i64 },
        quote! { pub committed: i64 },
        quote! { pub payload: String },
    ] {
        named.named.push(syn::Field::parse_named.parse2(tokens)?);
    }

    let name = s.ident.clone();
    let partition_lit = LitStr::new(&partition.to_string(), proc_macro2::Span::call_site());
    let derive_input: DeriveInput = syn::parse2(quote! { #s })?;
    let row = expand_row(derive_input, RowArgs { table: None })?;
    Ok(quote! {
        #row

        impl ::tables::ProjectionLog for #name {
            const PARTITION_COLUMN: &'static str = #partition_lit;

            fn seq(&self) -> i64 {
                self.seq
            }
            fn committed(&self) -> i64 {
                self.committed
            }
            fn payload(&self) -> &str {
                &self.payload
            }
        }
    })
}

struct ProjectionArgs {
    id: Option<String>,
    outputs: Vec<syn::Path>,
    reads: Vec<syn::Path>,
}

impl Parse for ProjectionArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut id: Option<String> = None;
        let mut outputs: Option<Vec<syn::Path>> = None;
        let mut reads: Option<Vec<syn::Path>> = None;

        while !input.is_empty() {
            let ident: Ident = input.parse()?;
            match ident.to_string().as_str() {
                "id" => {
                    let _: Token![=] = input.parse()?;
                    let lit: LitStr = input.parse()?;
                    id = Some(lit.value());
                }
                "key" => {
                    return Err(syn::Error::new(
                        ident.span(),
                        "projection: `key = ...` was removed — the partition \
                         comes from the log row type (`tables::ProjectionLog`)",
                    ));
                }
                "outputs" => outputs = Some(parse_path_list(input)?),
                "reads" => reads = Some(parse_path_list(input)?),
                other => {
                    return Err(syn::Error::new(
                        ident.span(),
                        format!(
                            "projection: unknown key `{other}` — expected \
                             `id`, `outputs`, `reads`"
                        ),
                    ));
                }
            }
            if input.is_empty() {
                break;
            }
            let _: Token![,] = input.parse()?;
        }

        let outputs = outputs
            .filter(|o| !o.is_empty())
            .ok_or_else(|| {
                syn::Error::new(input.span(), "projection: missing `outputs(RowType, ...)`")
            })?;
        Ok(Self { id, outputs, reads: reads.unwrap_or_default() })
    }
}

fn parse_path_list(input: ParseStream) -> syn::Result<Vec<syn::Path>> {
    let content;
    syn::parenthesized!(content in input);
    let paths = content.parse_terminated(syn::Path::parse, Token![,])?;
    Ok(paths.into_iter().collect())
}

fn expand_projection(
    input: syn::ItemImpl,
    args: ProjectionArgs,
) -> syn::Result<TokenStream2> {
    if input.trait_.is_some() {
        return Err(Error::new_spanned(
            &input,
            "#[projection] goes on an inherent impl block, not a trait impl",
        ));
    }
    if !input.generics.params.is_empty() {
        return Err(Error::new_spanned(
            &input.generics,
            "#[projection] does not support generics",
        ));
    }
    let Type::Path(TypePath { path, .. }) = input.self_ty.as_ref() else {
        return Err(Error::new_spanned(
            &input.self_ty,
            "#[projection] impl type must be a plain identifier",
        ));
    };
    let name = path
        .get_ident()
        .cloned()
        .ok_or_else(|| {
            Error::new_spanned(path, "#[projection] impl type must be a plain identifier")
        })?;

    if input
        .items
        .iter()
        .any(|item| matches!(item, syn::ImplItem::Fn(f) if f.sig.ident == "project"))
    {
        return Err(Error::new_spanned(
            &input,
            "#[projection]: `project` was removed — THE contract is the fold: \
             `apply(&mut self, row: &LogRow)` + \
             `render(&self, ctx: &RenderCtx<'_>, out: &mut Out)` \
             on the state type",
        ));
    }

    let find_fn = |fn_name: &str| {
        input.items.iter().find_map(|item| match item {
            syn::ImplItem::Fn(f) if f.sig.ident == fn_name => Some(f),
            _ => None,
        })
    };
    let (apply_fn, render_fn) = match (find_fn("apply"), find_fn("render")) {
        (Some(apply), Some(render)) => (apply, render),
        (Some(_), None) => {
            return Err(Error::new_spanned(
                &input,
                "#[projection]: `apply` without `render` — a projection needs both",
            ));
        }
        (None, Some(_)) => {
            return Err(Error::new_spanned(
                &input,
                "#[projection]: `render` without `apply` — a projection needs both",
            ));
        }
        (None, None) => {
            return Err(Error::new_spanned(
                &input,
                "#[projection] impl needs \
                 `fn apply(&mut self, row: &LogRow)` + \
                 `fn render(&self, ctx: &RenderCtx<'_>, out: &mut Out)` — \
                 the impl target is the fold state",
            ));
        }
    };

    // The impl target IS the state; `apply` carries the log row type.
    let apply_err = || {
        Error::new_spanned(
            &apply_fn.sig,
            "#[projection]: `apply` must take `&mut self, row: &LogRow` — \
             `self` is the fold state, the row is the generated log row \
             (seq/committed are fold input)",
        )
    };
    let mut apply_args = apply_fn.sig.inputs.iter();
    let row_ty = match (apply_args.next(), apply_args.next(), apply_args.next()) {
        (Some(FnArg::Receiver(recv)), Some(FnArg::Typed(row)), None) => {
            if recv.reference.is_none() || recv.mutability.is_none() {
                return Err(apply_err());
            }
            shared_ref_elem(&row.ty).ok_or_else(apply_err)?
        }
        _ => return Err(apply_err()),
    };

    // `render`'s param types are enforced by rustc at the shim's call
    // site; check the shape here for a pointed message.
    let render_err = || {
        Error::new_spanned(
            &render_fn.sig,
            "#[projection]: `render` must take \
             `&self, ctx: &RenderCtx<'_>, out: &mut Out`",
        )
    };
    let mut render_args = render_fn.sig.inputs.iter();
    match (
        render_args.next(),
        render_args.next(),
        render_args.next(),
        render_args.next(),
    ) {
        (Some(FnArg::Receiver(recv)), Some(FnArg::Typed(_)), Some(FnArg::Typed(_)), None) => {
            if recv.reference.is_none() || recv.mutability.is_some() {
                return Err(render_err());
            }
        }
        _ => return Err(render_err()),
    }

    let id_lit = LitStr::new(
        &args.id.unwrap_or_else(|| pascal_to_snake(&name.to_string())),
        proc_macro2::Span::call_site(),
    );
    let outputs = &args.outputs;
    let reads = &args.reads;

    Ok(quote! {
        #input

        impl ::database_projection::Projection for #name {
            fn spec(&self) -> ::database_projection::ProjectionSpec {
                ::database_projection::ProjectionSpec {
                    id: #id_lit.into(),
                    sources: ::std::vec![
                        ::database_projection::PartitionedSource {
                            table: <#row_ty as ::sql_engine::DbTable>::TABLE.into(),
                            partition_column: ::database_projection::typed::partition_column_index::<#row_ty>(
                                <#row_ty as ::tables::ProjectionLog>::PARTITION_COLUMN,
                            ),
                        }
                    ],
                    reads: ::std::vec![
                        #( <#reads as ::sql_engine::DbTable>::TABLE.into() ),*
                    ],
                    outputs: ::std::vec![
                        #( <#outputs as ::sql_engine::DbTable>::TABLE.into() ),*
                    ],
                }
            }

            fn project(
                &self,
                _partition: &::sql_engine::storage::CellValue,
                inputs: &::database_projection::Inputs,
                ctx: &::database_projection::ReadCtx<'_>,
                cache: &mut ::database_projection::FoldCache,
            ) -> ::core::result::Result<
                ::std::vec::Vec<::database_projection::OutputRow>,
                ::std::string::String,
            > {
                // §9.4 contract bound: the snapshot below clones the state.
                fn __state_contract<T: ::core::default::Default + ::core::clone::Clone>() {}
                __state_contract::<#name>();

                let __rows: ::std::vec::Vec<#row_ty> =
                    ::database_projection::typed::decode_rows(
                        inputs.rows(<#row_ty as ::sql_engine::DbTable>::TABLE),
                    )?;
                let __ordered = <#row_ty as ::tables::ProjectionLog>::in_fold_order(&__rows);
                let __committed_len = __ordered
                    .iter()
                    .take_while(|__r| <#row_ty as ::tables::ProjectionLog>::is_committed(__r))
                    .count();
                let __seqs: ::std::vec::Vec<i64> = __ordered[..__committed_len]
                    .iter()
                    .map(|__r| <#row_ty as ::tables::ProjectionLog>::seq(__r))
                    .collect();

                // §9.3 execution: resume from the committed-prefix memo when
                // the current committed seq list still extends it; anything
                // else (backfill, delete, first run) folds from zero.
                let (mut __state, __start): (#name, usize) = match cache
                    .get::<::database_projection::typed::FoldSnapshot<#name>>()
                {
                    ::core::option::Option::Some(__snap)
                        if __seqs.starts_with(&__snap.seqs) =>
                    {
                        (__snap.state.clone(), __snap.seqs.len())
                    }
                    _ => (::core::default::Default::default(), 0),
                };
                for __row in &__ordered[__start..__committed_len] {
                    __state.apply(__row)?;
                }
                if __start < __committed_len {
                    cache.put(::database_projection::typed::FoldSnapshot {
                        seqs: __seqs,
                        state: __state.clone(),
                    });
                }
                for __row in &__ordered[__committed_len..] {
                    __state.apply(__row)?;
                }
                let __ctx = ::database_projection::typed::RenderCtx::new(ctx);
                let mut __out = ::database_projection::typed::Out::new();
                __state.render(&__ctx, &mut __out)?;
                ::core::result::Result::Ok(__out.into_rows())
            }
        }
    })
}

/// `&T` (shared) → `Some(T)`, anything else → `None`.
fn shared_ref_elem(ty: &Type) -> Option<Type> {
    let Type::Reference(r) = ty else {
        return None;
    };
    if r.mutability.is_some() {
        return None;
    }
    Some((*r.elem).clone())
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
