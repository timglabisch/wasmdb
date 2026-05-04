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

    // Direct rpc-command-derive + rpc-command runtime to emit TS into the
    // turborepo `invoice-demo-generated` package source dir. Path is relative
    // to this crate's manifest dir (`examples/invoice-demo/shared/domain/`).
    let manifest = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    let target = std::path::Path::new(&manifest)
        .join("../../frontend/packages/generated/src");
    // Don't canonicalize — target may not exist yet on first build; ts-rs /
    // fs::create_dir_all will create it.
    println!("cargo:rustc-env=RPC_COMMAND_TS_ROOT={}", target.display());
    println!("cargo:rerun-if-changed=build.rs");
}
