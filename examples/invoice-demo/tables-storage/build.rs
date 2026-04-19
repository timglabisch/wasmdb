//! Generate server-side `Params` + `Fetcher` + `register_*` glue from
//! the `#[row]` / `#[query]` decls in `src/`. See `tables-codegen`.

fn main() {
    tables_codegen::Builder::new()
        .source_root("src")
        .server()
        .ctx_type("crate::AppCtx")
        .compile()
        .expect("tables-codegen (server mode)");
}
