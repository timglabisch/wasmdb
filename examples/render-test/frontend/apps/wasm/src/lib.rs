//! cdylib-only crate. Surface mirrors invoice-demo: the
//! `define_wasm_api!` macro from `sync_client` expands here, referencing
//! the codegen-emitted `register_all_tables` and (empty)
//! `register_all_requirements`.

#[cfg(target_arch = "wasm32")]
mod app {
    use render_test_domain::RenderTestCommand;

    mod generated {
        include!(concat!(env!("OUT_DIR"), "/generated.rs"));
    }

    sync_client::define_wasm_api!(
        command = RenderTestCommand,
        setup_db = generated::register_all_tables,
        register_requirements = generated::register_all_requirements,
    );
}
