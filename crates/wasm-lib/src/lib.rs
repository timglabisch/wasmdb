use std::cell::UnsafeCell;
use std::collections::HashMap;
use serde::Serialize;
use wasm_bindgen::prelude::*;

const BUFFER_SIZE: usize = 64 * 1024;

struct SharedBuffer(UnsafeCell<[u8; BUFFER_SIZE]>);
unsafe impl Sync for SharedBuffer {}

impl SharedBuffer {
    const fn new() -> Self {
        Self(UnsafeCell::new([0; BUFFER_SIZE]))
    }

    fn ptr(&self) -> *mut u8 {
        self.0.get() as *mut u8
    }

    fn write_bytes(&self, data: &[u8]) {
        let len = data.len().min(BUFFER_SIZE - 4);
        let buf = unsafe { &mut *self.0.get() };
        buf[..4].copy_from_slice(&(len as u32).to_le_bytes());
        buf[4..4 + len].copy_from_slice(&data[..len]);
    }
}

/// Buffer: Rust schreibt Diffs, TS liest.
static RUST_TO_TS: SharedBuffer = SharedBuffer::new();

// --- Diff Log ---

#[derive(Serialize, Clone)]
struct Diff {
    version: u32,
    table: String,
    id: String,
    key: String,
    value: String,
    diff: i8, // +1 insert, -1 delete
}

/// table -> id -> key/value
type Db = HashMap<String, HashMap<String, HashMap<String, String>>>;

struct DiffLog {
    diffs: Vec<Diff>,
    next_version: u32,
    db: Db,
}

impl DiffLog {
    fn new() -> Self {
        Self { diffs: Vec::new(), next_version: 1, db: HashMap::new() }
    }

    fn append(&mut self, table: &str, id: &str, key: &str, value: &str, diff: i8) {
        self.diffs.push(Diff {
            version: self.next_version,
            table: table.to_string(),
            id: id.to_string(),
            key: key.to_string(),
            value: value.to_string(),
            diff,
        });
        self.next_version += 1;

        // Materialisierte View mitführen
        if diff > 0 {
            self.db.entry(table.to_string())
                .or_default()
                .entry(id.to_string())
                .or_default()
                .insert(key.to_string(), value.to_string());
        } else {
            if let Some(t) = self.db.get_mut(table) {
                if let Some(row) = t.get_mut(id) {
                    row.remove(key);
                    if row.is_empty() { t.remove(id); }
                }
                if t.is_empty() { self.db.remove(table); }
            }
        }
    }

    fn since(&self, version: u32) -> &[Diff] {
        match self.diffs.iter().position(|d| d.version >= version) {
            Some(pos) => &self.diffs[pos..],
            None => &[],
        }
    }

    fn get_row(&self, table: &str, id: &str) -> Option<&HashMap<String, String>> {
        self.db.get(table).and_then(|t| t.get(id))
    }
}

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
    let new_row: HashMap<String, String> = serde_wasm_bindgen::from_value(data)?;
    let old_row: HashMap<String, String> = log().get_row(table, id)
        .cloned()
        .unwrap_or_default();

    // Alles was vorher da war und jetzt anders oder weg ist: -1
    for (key, old_val) in &old_row {
        if new_row.get(key) != Some(old_val) {
            log().append(table, id, key, "", -1);
        }
    }

    // Alles was jetzt da ist und vorher anders oder nicht da war: +1
    for (key, value) in &new_row {
        if old_row.get(key) != Some(value) {
            log().append(table, id, key, value, 1);
        }
    }

    Ok(())
}

/// TS ruft sync(since_version) auf. Rust schreibt alle Diffs seit
/// dieser Version in den Shared Buffer.
#[wasm_bindgen]
pub fn sync(since_version: u32) -> u32 {
    let diffs = log().since(since_version);
    let json = serde_json::to_vec(diffs).unwrap_or_default();
    RUST_TO_TS.write_bytes(&json);
    log().next_version
}
