//! First-class requirements for wasmdb — typed, identifiable data sources.
//!
//! See `wasmdb-requirements-design.md` for the full design. This crate
//! currently holds the static surface that was carved out of `sql-engine`:
//! requirement metadata, the boot-time registry, the `DbRequirement`
//! trait, and the runtime closure types. Lifetime / refcount / GC /
//! state-machine logic lands here in subsequent steps.

pub mod meta;
pub mod registry;
pub mod requirement;
pub mod runtime;
pub mod store;

pub use meta::{RequirementMeta, RequirementParamDef};
pub use registry::{Requirement, RequirementRegistry};
pub use requirement::DbRequirement;
pub use runtime::{RequirementFn, RequirementFuture};
pub use store::{
    make_derived_key, make_fetched_key, FetchDispatcher, FetchError, RequirementKey,
    RequirementStore, Slot, SlotKind, SlotState, SubscriberId,
};
