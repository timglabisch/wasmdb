//! Generate client-side Row + `register_all_tables` +
//! `register_all_projections` + wasm bindings from the `shared/domain`
//! crate's `#[row]` / `#[projection_row]` / `#[projection]` declarations.
//!
//! `projections_path` names the domain crate as seen from THIS crate: the
//! projection types carry function bodies, so `register_all_projections`
//! references them in place (unlike rows, which are re-emitted).

fn main() {
    tables_codegen::Builder::new()
        .source_root("../../../shared/domain/src")
        .client()
        .url("/table-fetch")
        .wasm_bindings(true)
        .projections_path("::projection_demo_domain")
        .ts_requirements_out("../../packages/generated/src/requirements.ts")
        .ts_rows_out("../../packages/generated/src/tables")
        .compile()
        .expect("tables-codegen (client mode)");
}
