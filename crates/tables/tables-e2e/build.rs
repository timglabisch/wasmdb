//! Run `tables-codegen` in server mode over our own `src/` tree. Generates
//! `Params` structs, `impl DbCaller`, `impl Fetcher`, and `register_{fn}`
//! glue for every `#[query]` under `src/`. Emits to `$OUT_DIR/generated.rs`.

fn main() {
    tables_codegen::Builder::new()
        .source_root("src")
        .server()
        .ctx_type("crate::AppCtx")
        .compile()
        .expect("tables-codegen (server mode)");
}
