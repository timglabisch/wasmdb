mod buffer;
mod query;
mod diff;
mod projection;
mod difflog;

use std::cell::UnsafeCell;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

use buffer::{SharedBuffer, BUFFER_SIZE};
use query::{ProjectionConfig, Row};
use difflog::DiffLog;

static RUST_TO_TS: SharedBuffer = SharedBuffer::new();

struct State(UnsafeCell<Option<DiffLog>>);
unsafe impl Sync for State {}

static LOG: State = State(UnsafeCell::new(None));

fn log() -> &'static mut DiffLog {
    unsafe { (*LOG.0.get()).get_or_insert_with(DiffLog::new) }
}

// --- WASM exports ---

#[wasm_bindgen]
pub fn rust_to_ts_ptr() -> *const u8 {
    RUST_TO_TS.ptr()
}

#[wasm_bindgen]
pub fn rust_to_ts_len() -> usize {
    BUFFER_SIZE
}

#[wasm_bindgen]
pub fn add(table: &str, id: &str, data: JsValue) -> Result<(), JsError> {
    let new_row: Row = serde_wasm_bindgen::from_value(data)?;
    let old_row: Row = log().get_row(table, id)
        .cloned()
        .unwrap_or_default();

    for (key, old_val) in &old_row {
        if new_row.get(key) != Some(old_val) {
            log().append(table, id, key, "", -1);
        }
    }

    for (key, value) in &new_row {
        if old_row.get(key) != Some(value) {
            log().append(table, id, key, value, 1);
        }
    }

    log().evaluate_projections(table, id, &old_row, &new_row);

    Ok(())
}

#[wasm_bindgen]
pub fn register_projection(config: JsValue, callback: JsValue) -> Result<u32, JsError> {
    let config: ProjectionConfig = serde_wasm_bindgen::from_value(config)?;
    let callback: js_sys::Function = callback.dyn_into()
        .map_err(|_| JsError::new("callback must be a function"))?;
    Ok(log().register_projection(config, callback))
}

#[wasm_bindgen]
pub fn unregister_projection(id: u32) {
    log().unregister_projection(id);
}

#[wasm_bindgen]
pub fn sync(since_version: u32) -> u32 {
    let diffs = log().since(since_version);
    let json = serde_json::to_vec(diffs).unwrap_or_default();
    RUST_TO_TS.write_bytes(&json);
    let next = log().next_version();
    log().flush_projections();
    next
}
