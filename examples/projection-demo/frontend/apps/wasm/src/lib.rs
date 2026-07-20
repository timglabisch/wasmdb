//! cdylib-only crate — the entire surface is wasm32. The
//! `sync_client::define_wasm_api!` macro and the codegen-emitted
//! `register_all_tables` / `register_all_projections` all live behind
//! `cfg(target_arch = "wasm32")`, so on host targets this lib is empty.
//!
//! Unlike the other examples, this one wires `projections = ...`: the
//! generated `register_all_projections` installs `BalanceFold` into the
//! client's projection engine, which maintains the derived `balance`
//! table at the notify chokepoint.

#[cfg(target_arch = "wasm32")]
mod app {
    use projection_demo_domain::ledger::activity_fold::ActivityFold;
    use projection_demo_domain::ProjectionDemoCommand;

    mod generated {
        include!(concat!(env!("OUT_DIR"), "/generated.rs"));
    }

    /// The generated engine (static `#[projection]` folds) plus the
    /// hand-registered dynamic `activity` template (design §12 — codegen
    /// does not pick `#[dynamic_projection]` impls up, so the wrapper adds
    /// it here). Instances are driven from JS via `projection_activate` /
    /// `projection_deactivate`.
    fn projections() -> sync_client::database_projection::ProjectionEngine {
        let mut engine = generated::register_all_projections();
        engine
            .register_dynamic(Box::new(ActivityFold::default()))
            .expect("register dynamic projection ActivityFold");
        engine
    }

    sync_client::define_wasm_api!(
        command = ProjectionDemoCommand,
        setup_db = generated::register_all_tables,
        register_requirements = generated::register_all_requirements,
        projections = projections,
    );
}
