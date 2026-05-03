//! Embedder-side `FetchDispatcher` for the wasm client.
//!
//! Bridges the synchronous `RequirementStore` to the wasm event loop:
//! looks up the codegen-emitted closure in `RequirementRegistry`,
//! spawns it via `wasm_bindgen_futures::spawn_local` (with an
//! abort handle so `cancel` can drop the work), and applies the result
//! back to the store. Each apply returns the keys whose state changed;
//! the dispatcher fans those out to a single `on_changed` callback so
//! the wasm-bindings layer can ping the per-subscriber JS callbacks.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use futures::future::{AbortHandle, Abortable, Aborted};
use requirements::{
    FetchDispatcher, FetchError, RequirementKey, RequirementRegistry, RequirementStore, SlotKind,
};
use wasm_bindgen_futures::spawn_local;

pub type OnChanged = Rc<dyn Fn(&RequirementKey)>;

pub struct WasmDispatcher {
    registry: Rc<RequirementRegistry>,
    store: Rc<RefCell<RequirementStore>>,
    on_changed: OnChanged,
    inflight: HashMap<RequirementKey, AbortHandle>,
}

impl WasmDispatcher {
    pub fn new(
        registry: Rc<RequirementRegistry>,
        store: Rc<RefCell<RequirementStore>>,
        on_changed: OnChanged,
    ) -> Self {
        Self {
            registry,
            store,
            on_changed,
            inflight: HashMap::new(),
        }
    }
}

impl FetchDispatcher for WasmDispatcher {
    fn dispatch(&mut self, key: &RequirementKey, kind: &SlotKind, generation: u64) {
        let SlotKind::Fetched { registered_id, args } = kind else {
            return;
        };
        let Some(fetcher) = self.registry.fetchers.get(registered_id.as_ref()) else {
            return;
        };
        let fetcher = fetcher.clone();
        let args = args.clone();
        let key_owned = key.clone();
        let store = self.store.clone();
        let on_changed = self.on_changed.clone();

        let (handle, reg) = AbortHandle::new_pair();
        let work = async move {
            let result = fetcher(args).await;
            let changed = match result {
                Ok(()) => store.borrow_mut().apply_ready(&key_owned, generation),
                Err(s) => store
                    .borrow_mut()
                    .apply_error(&key_owned, generation, FetchError::Network(s)),
            };
            for k in &changed {
                on_changed(k);
            }
        };
        spawn_local(async move {
            match Abortable::new(work, reg).await {
                Ok(()) => {}
                Err(Aborted) => {}
            }
        });

        if let Some(prev) = self.inflight.insert(key.clone(), handle) {
            prev.abort();
        }
    }

    fn cancel(&mut self, key: &RequirementKey, _generation: u64) {
        if let Some(handle) = self.inflight.remove(key) {
            handle.abort();
        }
    }
}
