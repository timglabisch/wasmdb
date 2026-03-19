use wasm_bindgen::prelude::*;
use std::cell::UnsafeCell;

const BUFFER_SIZE: usize = 1024;

struct SharedBuffer(UnsafeCell<[u8; BUFFER_SIZE]>);
unsafe impl Sync for SharedBuffer {}

impl SharedBuffer {
    const fn new() -> Self {
        Self(UnsafeCell::new([0; BUFFER_SIZE]))
    }

    fn ptr(&self) -> *mut u8 {
        self.0.get() as *mut u8
    }

    fn get(&self, index: usize) -> u8 {
        unsafe { (*self.0.get())[index] }
    }

    fn set(&self, index: usize, value: u8) {
        unsafe { (*self.0.get())[index] = value; }
    }
}

/// Buffer where TypeScript writes, Rust reads.
static TS_TO_RUST: SharedBuffer = SharedBuffer::new();

/// Buffer where Rust writes, TypeScript reads.
static RUST_TO_TS: SharedBuffer = SharedBuffer::new();

// --- ts_to_rust buffer (TS schreibt, Rust liest) ---

#[wasm_bindgen]
pub fn ts_to_rust_ptr() -> *mut u8 {
    TS_TO_RUST.ptr()
}

#[wasm_bindgen]
pub fn ts_to_rust_len() -> usize {
    BUFFER_SIZE
}

// --- rust_to_ts buffer (Rust schreibt, TS liest) ---

#[wasm_bindgen]
pub fn rust_to_ts_ptr() -> *const u8 {
    RUST_TO_TS.ptr()
}

#[wasm_bindgen]
pub fn rust_to_ts_len() -> usize {
    BUFFER_SIZE
}

/// TS ruft diese Funktion auf nachdem es in ts_to_rust geschrieben hat.
/// Rust spiegelt den Inhalt nach rust_to_ts.
#[wasm_bindgen]
pub fn sync() {
    for i in 0..BUFFER_SIZE {
        RUST_TO_TS.set(i, TS_TO_RUST.get(i));
    }
}
