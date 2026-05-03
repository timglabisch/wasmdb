//! Generate client-side Row + Fetcher + call-wrapper + wasm bindings
//! from the `shared/domain` crate's `#[row]` / `#[query]` declarations.
//! See `tables-codegen`.

fn main() {
    tables_codegen::Builder::new()
        .source_root("../../shared/domain/src")
        .client()
        .url("/table-fetch")
        .wasm_bindings(true)
        .ts_requirements_out("../ui/src/generated/requirements.ts")
        .ts_rows_out("../ui/src/generated/tables")
        .compile()
        .expect("tables-codegen (client mode)");
}
