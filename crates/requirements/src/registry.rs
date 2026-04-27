//! Boot-time registry of requirements: id → meta + fetch closure.
//!
//! Renamed from the engine's old `Caller` / `CallerRegistry`. Stores
//! the planner-side meta and the runtime fetch closure as parallel maps
//! so consumers can take disjoint borrows without a projection step.
//!
//! [`Requirement`] keeps the two halves bundled at the registration
//! API boundary so half-registration is impossible.

use std::collections::HashMap;

use crate::meta::RequirementMeta;
use crate::runtime::RequirementFn;

/// A single registered requirement: planner meta + async fetch closure,
/// keyed by `id` (wire-form `"{schema}::{function}"`).
pub struct Requirement {
    pub id: String,
    pub meta: RequirementMeta,
    pub fetcher: RequirementFn,
}

impl Requirement {
    pub fn new(
        id: impl Into<String>,
        meta: RequirementMeta,
        fetcher: RequirementFn,
    ) -> Self {
        Self { id: id.into(), meta, fetcher }
    }
}

/// Boot-time registry of all known requirements. Stores planner view
/// (`metas`) and runtime view (`fetchers`) as parallel `pub` maps so
/// consumers can take disjoint field borrows. `Clone` shares closure
/// identity via `Arc` inside [`RequirementFn`].
#[derive(Clone, Default)]
pub struct RequirementRegistry {
    /// Planner-side meta: `id → RequirementMeta`. Consumed by the runtime
    /// store for `writes` / policy resolution.
    pub metas: HashMap<String, RequirementMeta>,
    /// Runtime-side closures: `id → RequirementFn`.
    pub fetchers: HashMap<String, RequirementFn>,
}

impl RequirementRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, requirement: Requirement) {
        let Requirement { id, meta, fetcher } = requirement;
        self.metas.insert(id.clone(), meta);
        self.fetchers.insert(id, fetcher);
    }
}
