//! Wasm-side surface used by browser apps. Builds only for
//! `target_arch = "wasm32"` (gated at the parent `lib.rs`).

pub mod api;
pub mod debug;
pub mod exec;
pub mod req_bindings;
pub mod req_dispatcher;
pub mod state;
pub mod stream;

// Re-export the most-used handles so app code can `use sync_client::wasm::*`.
pub use api::{columns_to_rows, js_to_param_value, js_to_params, make_manual_promise};
pub use exec::{
    create_stream_for, execute_for, execute_on_stream_for, flush_stream_for, init_for,
};
pub use req_bindings::{install_requirements, no_register_requirements};
pub use state::{install_client, with_client, with_client_dyn, DynClient};

/// Generate the `#[wasm_bindgen]` exports that depend on the app's
/// command type. Place this once at the top level of the app's
/// `cdylib` crate. The macro emits:
///
/// - `init` — install client, register tables (via the user-supplied
///   setup fn), install streams, install requirements
/// - `create_stream`
/// - `execute` / `execute_on_stream`
/// - `flush_stream`
///
/// The generic-free exports (`subscribe`, `unsubscribe`, `on_dirty`,
/// `next_dirty`, `query`, `query_async`, `query_confirmed`,
/// `requirements_*`) live directly in this crate and are picked up by
/// the cdylib build automatically.
///
/// # Example
///
/// ```ignore
/// fn setup_db(db: &mut database::Database) {
///     db.register_table::<Customer>().unwrap();
///     // ...
/// }
///
/// sync_client::define_wasm_api!(
///     command = MyCommand,
///     setup_db = setup_db,
///     register_requirements = my_codegen::register_all_requirements,
/// );
/// ```
#[macro_export]
macro_rules! define_wasm_api {
    (
        command = $cmd:ty,
        setup_db = $setup_db:path $(,)?
    ) => {
        $crate::define_wasm_api!(
            command = $cmd,
            setup_db = $setup_db,
            register_requirements = $crate::wasm::no_register_requirements,
        );
    };
    (
        command = $cmd:ty,
        setup_db = $setup_db:path,
        register_requirements = $register_req:path $(,)?
    ) => {
        #[::wasm_bindgen::prelude::wasm_bindgen]
        pub fn init() {
            let mut db = ::database::Database::new();
            $setup_db(&mut db);
            let client: $crate::SyncClient<$cmd> = $crate::SyncClient::new(db);
            $crate::wasm::install_client::<$cmd>(client);
            $crate::wasm::init_for::<$cmd>();
            $crate::wasm::install_requirements(
                |zset| {
                    $crate::wasm::with_client::<$cmd, _>(|client| {
                        client.db_mut().apply_zset(zset).map_err(|e| e.to_string())
                    })
                },
                $register_req,
            );
        }

        #[::wasm_bindgen::prelude::wasm_bindgen]
        pub fn create_stream(batch_count: u32, batch_wait_ms: u32, retry_count: u32) -> f64 {
            $crate::wasm::create_stream_for::<$cmd>(batch_count, batch_wait_ms, retry_count)
        }

        #[::wasm_bindgen::prelude::wasm_bindgen]
        pub fn execute(
            cmd_json: &str,
        ) -> ::core::result::Result<
            ::wasm_bindgen::JsValue,
            ::wasm_bindgen::JsError,
        > {
            $crate::wasm::execute_for::<$cmd>(cmd_json)
        }

        #[::wasm_bindgen::prelude::wasm_bindgen]
        pub fn execute_on_stream(
            stream_id: f64,
            cmd_json: &str,
        ) -> ::core::result::Result<
            ::wasm_bindgen::JsValue,
            ::wasm_bindgen::JsError,
        > {
            $crate::wasm::execute_on_stream_for::<$cmd>(stream_id, cmd_json)
        }

        #[::wasm_bindgen::prelude::wasm_bindgen]
        pub fn flush_stream(stream_id: f64) -> ::js_sys::Promise {
            $crate::wasm::flush_stream_for::<$cmd>(stream_id)
        }
    };
}
