//! Server-side codegen: scans `src/` for `#[row]` and `#[query]` decls and
//! emits `Params` + `Fetcher` + `register_*` glue into `$OUT_DIR/generated.rs`.
//! Included from `lib.rs` under `#[cfg(feature = "server")]`. The codegen
//! itself runs unconditionally (it is a textual scan that ignores cfg).

fn main() {
    tables_codegen::Builder::new()
        .source_root("src")
        .server()
        .ctx_type("crate::AppCtx")
        .compile()
        .expect("tables-codegen (server mode)");
}
