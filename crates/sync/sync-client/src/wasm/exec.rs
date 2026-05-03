//! Generic-over-`C` building blocks used by the `define_wasm_api!`
//! macro. These can't be `#[wasm_bindgen]` themselves (the JS ABI
//! must be monomorphic), so the macro emits a thin wrapper in the
//! app crate that pins `C` and calls down here.

use serde::de::DeserializeOwned;
use sync::command::Command;
use sync::protocol::StreamId;
use wasm_bindgen::prelude::*;
use wasmdb_debug::DebugEvent;

use crate::wasm::api::make_manual_promise;
use crate::wasm::state::{
    default_stream_id, set_default_stream_id, with_client, with_client_dyn,
};
use crate::wasm::stream::{
    do_flush_stream, install_handle, install_streams, is_done, not_in_flight,
    now_ms, push_flush_waiter, queue_fetch, try_drain_queue, PendingFetch,
};

/// Initialise streams + default stream for the chosen command type.
/// Called from the macro-generated `init` after the app has installed
/// its typed `SyncClient`.
pub fn init_for<C: Command + 'static>() {
    install_streams::<C>();
    let stream_id_val = with_client::<C, _>(|client| client.create_stream().0);
    install_handle::<C>(stream_id_val, 1, 0, 0);
    set_default_stream_id(stream_id_val);
}

pub fn create_stream_for<C: Command + 'static>(
    batch_count: u32,
    batch_wait_ms: u32,
    retry_count: u32,
) -> f64 {
    let stream_id = with_client_dyn(|client| client.create_stream());
    install_handle::<C>(
        stream_id.0,
        batch_count as usize,
        batch_wait_ms,
        retry_count,
    );
    stream_id.0 as f64
}

pub fn execute_on_stream_for<C>(stream_id: f64, cmd_json: &str) -> Result<JsValue, JsError>
where
    C: Command + DeserializeOwned + 'static,
{
    let stream_id_val = stream_id as u64;
    let cmd: C = serde_json::from_str(cmd_json).map_err(|e| JsError::new(&e.to_string()))?;

    let request = with_client::<C, _>(|client| {
        client
            .execute(StreamId(stream_id_val), cmd)
            .map_err(|e| JsError::new(&e.to_string()))
    })?;

    wasmdb_debug::log_event(DebugEvent::Execute {
        timestamp_ms: now_ms(),
        stream_id: stream_id_val,
        command_json: cmd_json.to_string(),
        zset_entry_count: request.client_zset.len(),
    });

    let zset_js = serde_wasm_bindgen::to_value(&request.client_zset)
        .map_err(|e| JsError::new(&e.to_string()))?;

    wasmdb_debug::track_table_invalidations(&request.client_zset);

    let (resolve, reject, confirmed) = make_manual_promise();

    queue_fetch::<C>(
        stream_id_val,
        PendingFetch {
            request,
            resolve,
            reject,
        },
    );

    try_drain_queue::<C>(stream_id_val);

    let result = js_sys::Object::new();
    js_sys::Reflect::set(&result, &"zset".into(), &zset_js)
        .map_err(|e| JsError::new(&format!("{e:?}")))?;
    js_sys::Reflect::set(&result, &"confirmed".into(), &confirmed)
        .map_err(|e| JsError::new(&format!("{e:?}")))?;
    Ok(result.into())
}

pub fn execute_for<C>(cmd_json: &str) -> Result<JsValue, JsError>
where
    C: Command + DeserializeOwned + 'static,
{
    let stream_id = default_stream_id();
    execute_on_stream_for::<C>(stream_id as f64, cmd_json)
}

pub fn flush_stream_for<C: Command + 'static>(stream_id: f64) -> js_sys::Promise {
    let stream_id_val = stream_id as u64;

    if is_done::<C>(stream_id_val) {
        return js_sys::Promise::resolve(&JsValue::UNDEFINED);
    }

    let (resolve, _reject, promise) = make_manual_promise();
    push_flush_waiter::<C>(stream_id_val, resolve);

    if not_in_flight::<C>(stream_id_val) {
        do_flush_stream::<C>(stream_id_val, true);
    }

    promise
}
