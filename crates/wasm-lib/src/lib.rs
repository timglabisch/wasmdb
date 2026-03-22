mod buffer;
mod query;
mod projection;
mod schema;
mod diff_log;
mod view;
mod database;

use std::cell::UnsafeCell;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

use buffer::{SharedBuffer, read_u32_le};
use query::ProjectionQuery;
use database::Database;

const TS_TO_RUST_SIZE: usize = 1024 * 1024;

static TS_TO_RUST: SharedBuffer<TS_TO_RUST_SIZE> = SharedBuffer::new();

struct State(UnsafeCell<Option<Database>>);
unsafe impl Sync for State {}

static DB: State = State(UnsafeCell::new(None));

fn db() -> &'static mut Database {
    unsafe { (*DB.0.get()).get_or_insert_with(Database::new) }
}

fn process_ts_buffer() {
    let buf = TS_TO_RUST.as_slice();
    let from = read_u32_le(buf, 0) as usize;
    let to = read_u32_le(buf, 4) as usize;
    if from < to && to <= TS_TO_RUST_SIZE {
        db().process_buffer(buf, to);
    }
    TS_TO_RUST.reset_header();
}

// --- WASM exports ---

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
pub fn register_projection(query: JsValue, fields: JsValue, callback: JsValue) -> Result<u32, JsError> {
    let query: ProjectionQuery = serde_wasm_bindgen::from_value(query)?;
    let fields: Option<Vec<String>> = serde_wasm_bindgen::from_value(fields)?;
    let callback: js_sys::Function = callback.dyn_into()
        .map_err(|_| JsError::new("callback must be a function"))?;
    Ok(db().register_projection(query, fields, callback))
}

#[wasm_bindgen]
pub fn unregister_projection(id: u32) {
    db().unregister_projection(id);
}

#[wasm_bindgen]
pub fn register_table(table_name: &str, field_names: JsValue) -> u16 {
    let fields: Vec<String> = serde_wasm_bindgen::from_value(field_names).unwrap();
    db().register_table(table_name.to_string(), fields)
}

#[wasm_bindgen]
pub fn reset() {
    db().reset();
}

#[wasm_bindgen]
pub fn sync(since_version: u32) -> u32 {
    process_ts_buffer();
    db().sync();
    since_version
}

#[wasm_bindgen]
pub fn begin_tx() -> u64 {
    db().begin_tx()
}

#[wasm_bindgen]
pub fn revert_tx(tx_id: u64) {
    db().revert_tx(tx_id);
}
