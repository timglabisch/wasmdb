//! No tables-codegen here — render-test has no `#[query]` decls and the
//! echo-server doesn't need Fetcher impls. The wasm crate's build.rs
//! runs the client-mode codegen that produces `register_all_tables`.
//!
//! This build.rs only sets `RPC_COMMAND_TS_ROOT` so the rpc-command derive
//! emits TS into the turborepo `render-test-generated` package source dir.

fn main() {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    let target = std::path::Path::new(&manifest)
        .join("../../frontend/packages/generated/src");
    println!("cargo:rustc-env=RPC_COMMAND_TS_ROOT={}", target.display());
    println!("cargo:rerun-if-changed=build.rs");
}
