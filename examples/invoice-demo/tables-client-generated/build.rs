//! Generate client-side Row + Fetcher + call-wrapper + wasm bindings
//! from the sibling `tables-storage` crate's `#[row]` / `#[query]`
//! declarations. See `tables-codegen`.

fn main() {
    tables_codegen::Builder::new()
        .source_root("../tables-storage/src")
        .client()
        .url("/table-fetch")
        .wasm_bindings(true)
        .ts_requirements_out("../frontend/src/generated/requirements.ts")
        .ts_rows_out("../frontend/src/generated/tables")
        .compile()
        .expect("tables-codegen (client mode)");
}
