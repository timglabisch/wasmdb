//! Generate client-side Row + register_all_tables + wasm bindings from
//! the `shared/domain` crate. Mirrors invoice-demo.

fn main() {
    tables_codegen::Builder::new()
        .source_root("../../../shared/domain/src")
        .client()
        .url("/table-fetch")
        .wasm_bindings(true)
        .ts_requirements_out("../../packages/generated/src/requirements.ts")
        .ts_rows_out("../../packages/generated/src/tables")
        .compile()
        .expect("tables-codegen (client mode)");
}
