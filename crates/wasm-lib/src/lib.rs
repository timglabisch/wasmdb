mod buffer;
mod query;
mod diff;
mod projection;
mod difflog;

use std::cell::UnsafeCell;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

use buffer::SharedBuffer;
use query::{ProjectionConfig, new_row};
use difflog::DiffLog;

const TS_TO_RUST_SIZE: usize = 1024 * 1024;

static TS_TO_RUST: SharedBuffer<TS_TO_RUST_SIZE> = SharedBuffer::new();

struct State(UnsafeCell<Option<DiffLog>>);
unsafe impl Sync for State {}

static LOG: State = State(UnsafeCell::new(None));

fn log() -> &'static mut DiffLog {
    unsafe { (*LOG.0.get()).get_or_insert_with(DiffLog::new) }
}

// --- Buffer parsing ---

fn read_u16_le(buf: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([buf[offset], buf[offset + 1]])
}

fn read_u32_le(buf: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([buf[offset], buf[offset + 1], buf[offset + 2], buf[offset + 3]])
}

fn read_str(buf: &[u8], pos: &mut usize, end: usize) -> Option<String> {
    if *pos + 2 > end { return None; }
    let len = read_u16_le(buf, *pos) as usize;
    *pos += 2;
    if *pos + len > end { return None; }
    let s = unsafe { String::from_utf8_unchecked(buf[*pos..*pos + len].to_vec()) };
    *pos += len;
    Some(s)
}

fn process_ts_buffer() {
    let buf = TS_TO_RUST.as_slice();

    let from = read_u32_le(buf, 0) as usize;
    let to = read_u32_le(buf, 4) as usize;

    if from >= to || to > TS_TO_RUST_SIZE {
        return;
    }

    let mut pos = from;
    while pos < to {
        if pos + 2 > to { break; }
        let table_id = read_u16_le(buf, pos);
        pos += 2;

        let id = match read_str(buf, &mut pos, to) { Some(s) => s, None => break };

        // Copy field_ids (Vec<u16>, cheap) to avoid borrow conflict with set_row
        let field_ids = log().tables[table_id as usize].field_ids.clone();

        let mut row = new_row(field_ids.len());
        for fi in 0..field_ids.len() {
            let value = match read_str(buf, &mut pos, to) { Some(s) => s, None => break };
            row.insert(field_ids[fi], value);
        }

        log().set_row(table_id, id, row);
    }

    // Reset buffer header
    let buf_mut = unsafe { &mut *(TS_TO_RUST.ptr() as *mut [u8; TS_TO_RUST_SIZE]) };
    buf_mut[0..4].copy_from_slice(&8u32.to_le_bytes());
    buf_mut[4..8].copy_from_slice(&8u32.to_le_bytes());
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
pub fn register_table(table_name: &str, field_names: JsValue) -> u16 {
    let fields: Vec<String> = serde_wasm_bindgen::from_value(field_names).unwrap();
    log().register_table(table_name.to_string(), fields)
}

#[wasm_bindgen]
pub fn reset() {
    log().reset();
}

#[wasm_bindgen]
pub fn sync(since_version: u32) -> u32 {
    process_ts_buffer();
    log().sync();
    since_version
}

#[wasm_bindgen]
pub fn begin_tx() -> u64 {
    log().begin_tx()
}

#[wasm_bindgen]
pub fn revert_tx(tx_id: u64) {
    log().revert_tx(tx_id);
}
