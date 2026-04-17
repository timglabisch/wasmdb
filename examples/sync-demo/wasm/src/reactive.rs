use wasm_bindgen::prelude::*;

use database_reactive::{Callback, SubId};

use crate::debug::{bump_notification_count, log_event, now_ms, DebugEvent};

/// Bridge a JS `Function` into the reactive-database `Callback` contract.
///
/// The callback runs inside `ReactiveDatabase::notify` (synchronous), so we
/// dispatch to JS via `spawn_local` to avoid JS-side code re-entering WASM
/// while the `CLIENT` `RefCell` is still held mutably.
pub(crate) fn wrap_js_callback(js_callback: js_sys::Function) -> Callback {
    Box::new(move |sub_id: SubId, triggered: &[usize]| {
        bump_notification_count(sub_id.0);
        log_event(DebugEvent::Notification {
            timestamp_ms: now_ms(),
            sub_id: sub_id.0,
            triggered_count: triggered.len(),
        });
        let js_cb = js_callback.clone();
        wasm_bindgen_futures::spawn_local(async move {
            if let Err(e) = js_cb.call0(&JsValue::NULL) {
                web_sys::console::error_2(
                    &format!("subscription {} callback error:", sub_id.0).into(),
                    &e,
                );
            }
        });
    })
}
