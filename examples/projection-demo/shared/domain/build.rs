//! No tables-codegen here — the client-mode codegen (Row +
//! `register_all_tables` + `register_all_projections` + wasm bindings)
//! runs in the wasm crate's build.rs against this crate's source.
//!
//! This build.rs only sets `RPC_COMMAND_TS_ROOT` so the rpc-command derive
//! emits the command factory TS into the generated turborepo package.

fn main() {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    let target = std::path::Path::new(&manifest)
        .join("../../frontend/packages/generated/src");
    println!("cargo:rustc-env=RPC_COMMAND_TS_ROOT={}", target.display());
    println!("cargo:rerun-if-changed=build.rs");
}
