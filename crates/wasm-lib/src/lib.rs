use wasm_bindgen::prelude::*;
use std::sync::OnceLock;

static BUFFER: OnceLock<Vec<u8>> = OnceLock::new();

fn get_buffer() -> &'static Vec<u8> {
    BUFFER.get_or_init(|| {
        vec![42; 1024]
    })
}

#[wasm_bindgen]
pub fn buffer_ptr() -> *const u8 {
    get_buffer().as_ptr()
}

#[wasm_bindgen]
pub fn buffer_len() -> usize {
    get_buffer().len()
}

#[wasm_bindgen]
pub fn read_first_byte_from_rust() -> u8 {
    get_buffer()[0]
}
