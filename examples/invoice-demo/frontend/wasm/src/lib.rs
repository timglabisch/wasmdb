//! cdylib-only crate — the entire surface is wasm32. The
//! `sync_client::define_wasm_api!` macro and the codegen-emitted
//! `register_all_tables` / `register_all_requirements` both live behind
//! `cfg(target_arch = "wasm32")`, so on host targets this lib is empty.

#[cfg(target_arch = "wasm32")]
mod app {
    use invoice_demo_domain::InvoiceCommand;

    mod generated {
        include!(concat!(env!("OUT_DIR"), "/generated.rs"));
    }

    sync_client::define_wasm_api!(
        command = InvoiceCommand,
        setup_db = generated::register_all_tables,
        register_requirements = generated::register_all_requirements,
    );
}
