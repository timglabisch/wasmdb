mod buffer;
mod query;
mod diff;
mod projection;
mod difflog;

use std::cell::UnsafeCell;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

use buffer::SharedBuffer;
use query::{ProjectionConfig, Row};
use difflog::DiffLog;

const RUST_TO_TS_SIZE: usize = 16 * 1024 * 1024;
const TS_TO_RUST_SIZE: usize = 1024 * 1024;

static RUST_TO_TS: SharedBuffer<RUST_TO_TS_SIZE> = SharedBuffer::new();
static TS_TO_RUST: SharedBuffer<TS_TO_RUST_SIZE> = SharedBuffer::new();

struct State(UnsafeCell<Option<DiffLog>>);
unsafe impl Sync for State {}

static LOG: State = State(UnsafeCell::new(None));

fn log() -> &'static mut DiffLog {
    unsafe { (*LOG.0.get()).get_or_insert_with(DiffLog::new) }
}

// --- Buffer parsing ---

fn read_u32_le(buf: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([buf[offset], buf[offset + 1], buf[offset + 2], buf[offset + 3]])
}

fn process_ts_buffer() {
    let buf = TS_TO_RUST.as_slice();

    let from = read_u32_le(buf, 0) as usize;
    let to = read_u32_le(buf, 4) as usize;

    if from >= to || to > TS_TO_RUST_SIZE {
        return;
    }

    let mut pos = from;

    while pos + 4 <= to {
        let json_len = read_u32_le(buf, pos) as usize;
        pos += 4;

        if pos + json_len > to { break; }

        if let Ok((table, id, new_row)) =
            serde_json::from_slice::<(String, String, Row)>(&buf[pos..pos + json_len])
        {
            let old_row: Row = log().get_row(&table, &id)
                .cloned()
                .unwrap_or_default();

            for (key, old_val) in &old_row {
                if new_row.get(key) != Some(old_val) {
                    log().append(&table, &id, key, "", -1);
                }
            }

            for (key, value) in &new_row {
                if old_row.get(key) != Some(value) {
                    log().append(&table, &id, key, value, 1);
                }
            }

            log().evaluate_projections(&table, &id, &old_row, &new_row);
        }

        pos += json_len;
    }

    // Reset buffer header
    let buf_mut = unsafe { &mut *(TS_TO_RUST.ptr() as *mut [u8; TS_TO_RUST_SIZE]) };
    buf_mut[0..4].copy_from_slice(&8u32.to_le_bytes());
    buf_mut[4..8].copy_from_slice(&8u32.to_le_bytes());
}

// --- WASM exports ---

#[wasm_bindgen]
pub fn rust_to_ts_ptr() -> *const u8 {
    RUST_TO_TS.ptr()
}

#[wasm_bindgen]
pub fn rust_to_ts_len() -> usize {
    RUST_TO_TS_SIZE
}

#[wasm_bindgen]
pub fn ts_to_rust_ptr() -> *const u8 {
    TS_TO_RUST.ptr()
}

#[wasm_bindgen]
pub fn ts_to_rust_len() -> usize {
    TS_TO_RUST_SIZE
}

#[wasm_bindgen]
pub fn flush_ts_buffer() -> *const u8 {
    process_ts_buffer();
    TS_TO_RUST.ptr()
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
pub fn reset() {
    log().reset();
}

#[wasm_bindgen]
pub fn sync(since_version: u32) -> u32 {
    process_ts_buffer();
    let diffs = log().since(since_version);
    let json = serde_json::to_vec(diffs).unwrap_or_default();
    RUST_TO_TS.write_bytes(&json);
    let next = log().next_version();
    log().flush_projections();
    next
}
