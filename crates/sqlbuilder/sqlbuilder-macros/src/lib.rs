//! Proc-macros for the [`sqlbuilder`] crate. Users import these via the
//! `sqlbuilder` re-exports — depending on this crate directly is unusual.

mod from_row;
mod parse;
mod sql;

use proc_macro::TokenStream;

/// Build a `SqlStmt` from a SQL string with `{name}` placeholders.
///
/// Bare placeholders capture identifiers from the surrounding scope
/// (`format!`-style); explicit `name = expr` bindings override or supply
/// values that aren't simple locals. Engine-level `:name` placeholders
/// pass through unchanged.
///
/// # Examples
///
/// ```ignore
/// // capture from scope
/// let id = some_uuid;
/// sql!("DELETE FROM x WHERE id = {id}");
///
/// // explicit binding (field access doesn't auto-capture)
/// sql!("DELETE FROM x WHERE id = {id}", id = self.id);
///
/// // SQL placeholder name ≠ binding name
/// sql!("WHERE foo = {foo}", foo = some_var);
///
/// // compose: a fragment can be bound to another statement's placeholder
/// let cond = sql!("status = {status}", status = "active");
/// sql!("SELECT * FROM x WHERE {cond}", cond = cond);
/// ```
#[proc_macro]
pub fn sql(input: TokenStream) -> TokenStream {
    sql::expand(input.into()).unwrap_or_else(|e| e.to_compile_error()).into()
}

/// Derive `FromRow`: read columns positionally into named struct fields.
/// Each field's type must implement `sqlbuilder::FromCell`.
#[proc_macro_derive(FromRow)]
pub fn derive_from_row(input: TokenStream) -> TokenStream {
    let ast = syn::parse_macro_input!(input as syn::DeriveInput);
    from_row::expand(ast)
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}
