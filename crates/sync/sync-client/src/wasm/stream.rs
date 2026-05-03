//! Per-stream batching/queue/flush/retry — generic over the app's command
//! type. The thread-local `STREAM_HANDLES` is type-erased so non-generic
//! callers can release/check stream state without naming `C`.

use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;

use js_sys::Uint8Array;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;

use sync::command::Command;
use sync::protocol::{BatchCommandRequest, BatchCommandResponse, CommandRequest, Verdict};
use wasmdb_debug::DebugEvent;

use crate::wasm::state::with_client;

pub struct PendingFetch<C: Command> {
    pub request: CommandRequest<C>,
    pub resolve: js_sys::Function,
    pub reject: js_sys::Function,
}

pub struct StreamHandle<C: Command> {
    pub batch_count: usize,
    pub batch_wait_ms: u32,
    pub retry_count: u32,
    pub queue: Vec<PendingFetch<C>>,
    pub in_flight: bool,
    pub flush_waiters: Vec<js_sys::Function>,
    pub microtask_scheduled: bool,
}

impl<C: Command> StreamHandle<C> {
    pub fn new(batch_count: usize, batch_wait_ms: u32, retry_count: u32) -> Self {
        Self {
            batch_count: batch_count.max(1),
            batch_wait_ms,
            retry_count,
            queue: Vec::new(),
            in_flight: false,
            flush_waiters: Vec::new(),
            microtask_scheduled: false,
        }
    }
}

thread_local! {
    /// Holds `RefCell<HashMap<u64, StreamHandle<C>>>` for the app's chosen
    /// command type. Wrapped in `Box<dyn Any>` so the outer slot is non-
    /// generic; downcast on each access. The inner `RefCell` lets nested
    /// access patterns (drain → flush → finish_flush → drain again) borrow
    /// independently, matching the original demo's behavior.
    static STREAM_HANDLES: RefCell<Option<Box<dyn Any>>> = const { RefCell::new(None) };
}

pub fn install_streams<C: Command + 'static>() {
    let map: HashMap<u64, StreamHandle<C>> = HashMap::new();
    STREAM_HANDLES.with(|s| *s.borrow_mut() = Some(Box::new(RefCell::new(map))));
}

fn with_streams<C: Command + 'static, R>(
    f: impl FnOnce(&mut HashMap<u64, StreamHandle<C>>) -> R,
) -> R {
    STREAM_HANDLES.with(|s| {
        let borrow = s.borrow();
        let any_box = borrow
            .as_ref()
            .expect("sync-client wasm: streams not installed — call init() first");
        let cell: &RefCell<HashMap<u64, StreamHandle<C>>> = any_box
            .downcast_ref()
            .expect("sync-client wasm: stream type mismatch");
        let mut map = cell.borrow_mut();
        f(&mut *map)
    })
}

pub fn install_handle<C: Command + 'static>(
    stream_id: u64,
    batch_count: usize,
    batch_wait_ms: u32,
    retry_count: u32,
) {
    with_streams::<C, _>(|map| {
        map.insert(
            stream_id,
            StreamHandle::new(batch_count, batch_wait_ms, retry_count),
        );
    });
}

pub fn queue_fetch<C: Command + 'static>(stream_id: u64, item: PendingFetch<C>) {
    with_streams::<C, _>(|map| {
        let handle = map
            .get_mut(&stream_id)
            .expect("unknown stream — call create_stream() first");
        handle.queue.push(item);
    });
}

pub fn is_done<C: Command + 'static>(stream_id: u64) -> bool {
    with_streams::<C, _>(|map| {
        map.get(&stream_id)
            .map_or(true, |h| h.queue.is_empty() && !h.in_flight)
    })
}

pub fn push_flush_waiter<C: Command + 'static>(stream_id: u64, resolve: js_sys::Function) {
    with_streams::<C, _>(|map| {
        if let Some(h) = map.get_mut(&stream_id) {
            h.flush_waiters.push(resolve);
        }
    });
}

pub fn not_in_flight<C: Command + 'static>(stream_id: u64) -> bool {
    with_streams::<C, _>(|map| map.get(&stream_id).map_or(true, |h| !h.in_flight))
}

enum DrainAction {
    FlushNow,
    Schedule,
}

pub fn try_drain_queue<C: Command + 'static>(stream_id_val: u64) {
    let action = with_streams::<C, _>(|map| {
        let handle = map.get(&stream_id_val)?;
        if handle.in_flight || handle.queue.is_empty() {
            return None;
        }
        if handle.queue.len() >= handle.batch_count || handle.batch_count == 1 {
            return Some(DrainAction::FlushNow);
        }
        if handle.microtask_scheduled {
            return None;
        }
        Some(DrainAction::Schedule)
    });

    match action {
        Some(DrainAction::FlushNow) => do_flush_stream::<C>(stream_id_val, false),
        Some(DrainAction::Schedule) => schedule_flush::<C>(stream_id_val),
        None => {}
    }
}

fn schedule_flush<C: Command + 'static>(stream_id_val: u64) {
    let wait_ms = with_streams::<C, _>(|map| {
        let Some(h) = map.get_mut(&stream_id_val) else {
            return 0;
        };
        h.microtask_scheduled = true;
        h.batch_wait_ms
    });

    let run = move || {
        with_streams::<C, _>(|map| {
            if let Some(h) = map.get_mut(&stream_id_val) {
                h.microtask_scheduled = false;
            }
        });
        do_flush_stream::<C>(stream_id_val, false);
    };

    if wait_ms > 0 {
        let cb = wasm_bindgen::closure::Closure::once_into_js(run);
        if let Some(window) = web_sys::window() {
            let _ = window.set_timeout_with_callback_and_timeout_and_arguments_0(
                cb.unchecked_ref(),
                wait_ms as i32,
            );
        }
    } else {
        wasm_bindgen_futures::spawn_local(async move { run() });
    }
}

pub fn do_flush_stream<C: Command + 'static>(stream_id_val: u64, take_all: bool) {
    let (items, retry_count) = with_streams::<C, _>(|map| {
        let Some(handle) = map.get_mut(&stream_id_val) else {
            return (Vec::new(), 0);
        };
        let count = if take_all {
            handle.queue.len()
        } else {
            handle.batch_count.min(handle.queue.len())
        };
        let items: Vec<PendingFetch<C>> = handle.queue.drain(..count).collect();
        handle.in_flight = true;
        (items, handle.retry_count)
    });

    if items.is_empty() {
        finish_flush::<C>(stream_id_val);
        return;
    }

    let batch_request = BatchCommandRequest {
        requests: items.iter().map(|p| p.request.clone()).collect(),
    };
    let batch_bytes = match borsh::to_vec(&batch_request) {
        Ok(b) => b,
        Err(e) => {
            let err = JsValue::from_str(&format!("serialize batch: {e}"));
            for item in &items {
                let _ = item.reject.call1(&JsValue::NULL, &err);
            }
            finish_flush::<C>(stream_id_val);
            return;
        }
    };

    wasmdb_debug::log_event(DebugEvent::FetchStart {
        timestamp_ms: now_ms(),
        stream_id: stream_id_val,
        request_bytes: batch_bytes.len(),
    });

    wasm_bindgen_futures::spawn_local(async move {
        let fetch_start = now_ms();

        let mut last_err: Option<JsValue> = None;
        let mut response_bytes = None;
        for _attempt in 0..=retry_count {
            match do_fetch(&batch_bytes).await {
                Ok(bytes) => {
                    response_bytes = Some(bytes);
                    break;
                }
                Err(e) => last_err = Some(e),
            }
        }

        let fetch_end = now_ms();
        wasmdb_debug::log_event(DebugEvent::FetchEnd {
            timestamp_ms: fetch_end,
            stream_id: stream_id_val,
            response_bytes: response_bytes.as_ref().map_or(0, |b| b.len()),
            latency_ms: fetch_end - fetch_start,
        });

        match response_bytes {
            Some(bytes) => process_batch_response::<C>(stream_id_val, bytes, &items),
            None => {
                let err = last_err.unwrap_or_else(|| JsValue::from_str("fetch failed"));
                for item in &items {
                    let _ = item.reject.call1(&JsValue::NULL, &err);
                }
            }
        }

        finish_flush::<C>(stream_id_val);
    });
}

fn process_batch_response<C: Command + 'static>(
    stream_id_val: u64,
    bytes: Vec<u8>,
    items: &[PendingFetch<C>],
) {
    let batch_response: BatchCommandResponse = match borsh::from_slice(&bytes) {
        Ok(r) => r,
        Err(e) => {
            let err = JsValue::from_str(&format!("deserialize batch response: {e}"));
            for item in items {
                let _ = item.reject.call1(&JsValue::NULL, &err);
            }
            return;
        }
    };

    let first_reject = with_client::<C, _>(|client| {
        let mut first_reject: Option<String> = None;
        for response in batch_response.responses {
            if let Verdict::Rejected { ref reason } = response.verdict {
                if first_reject.is_none() {
                    first_reject = Some(reason.clone());
                }
            }
            let _ = client.receive_response(response);
        }
        first_reject
    });

    let result = js_sys::Object::new();
    match &first_reject {
        Some(reason) => {
            wasmdb_debug::log_event(DebugEvent::Rejected {
                timestamp_ms: now_ms(),
                stream_id: stream_id_val,
                reason: reason.clone(),
            });
            let _ = js_sys::Reflect::set(&result, &"status".into(), &"rejected".into());
            let _ = js_sys::Reflect::set(&result, &"reason".into(), &reason.clone().into());
        }
        None => {
            wasmdb_debug::log_event(DebugEvent::Confirmed {
                timestamp_ms: now_ms(),
                stream_id: stream_id_val,
            });
            let _ = js_sys::Reflect::set(&result, &"status".into(), &"confirmed".into());
        }
    }
    let result_val: JsValue = result.into();
    for item in items {
        let _ = item.resolve.call1(&JsValue::NULL, &result_val);
    }
}

fn finish_flush<C: Command + 'static>(stream_id_val: u64) {
    let waiters = with_streams::<C, _>(|map| {
        let Some(handle) = map.get_mut(&stream_id_val) else {
            return Vec::new();
        };
        handle.in_flight = false;
        if handle.queue.is_empty() {
            handle.flush_waiters.drain(..).collect()
        } else {
            Vec::new()
        }
    });
    for waiter in waiters {
        let _ = waiter.call0(&JsValue::NULL);
    }
    try_drain_queue::<C>(stream_id_val);
}

async fn do_fetch(body: &[u8]) -> Result<Vec<u8>, JsValue> {
    let opts = web_sys::RequestInit::new();
    opts.set_method("POST");
    let uint8_body = Uint8Array::from(body);
    opts.set_body(&uint8_body);

    let request = web_sys::Request::new_with_str_and_init("/command", &opts)?;
    request.headers().set("Content-Type", "application/octet-stream")?;

    let window = web_sys::window().ok_or_else(|| JsValue::from_str("no global window"))?;
    let resp_value = JsFuture::from(window.fetch_with_request(&request)).await?;
    let resp: web_sys::Response = resp_value.dyn_into()?;

    if !resp.ok() {
        let text = JsFuture::from(resp.text()?).await?;
        return Err(JsValue::from_str(&format!(
            "HTTP {}: {}",
            resp.status(),
            text.as_string().unwrap_or_default()
        )));
    }

    let buf = JsFuture::from(resp.array_buffer()?).await?;
    let uint8 = Uint8Array::new(&buf);
    Ok(uint8.to_vec())
}

pub fn now_ms() -> f64 {
    js_sys::Date::now()
}
