//! Reactive subscription runtime (execution + registry).
//!
//! The planning side lives in [`crate::planner::reactive`]; this module owns
//! the runtime-only pieces (subscription registry + per-mutation execution).
//!
//! ## Identity model
//!
//! Three types sit next to each other — their relationship is documented in
//! [`identity`]. In brief:
//!
//! - [`identity::SubscriptionKey`] — content identity, used for dedup.
//! - [`identity::SubscriptionId`] — runtime identity, stored in the reverse
//!   index and dirty-notification structures.
//! - [`identity::SubscriptionHandle`] — per-caller token, lives at FFI
//!   boundaries where safe double-unsubscribe matters.
//!
//! The registry in this module speaks `SubscriptionId` only. Dedup
//! (`SubscriptionKey` → `SubscriptionId`) and caller safety (`SubscriptionHandle`)
//! are handled one layer up by `database-reactive` and the wasm binding,
//! respectively.
//!
//! ## Flow
//!
//! ```text
//! 1. Plan (once per query)
//!    let plan = sql_engine::planner::reactive::plan_reactive(ast, &schemas)?;
//!        └─ planner::reactive::extract   — AST → logical conditions
//!        └─ planner::reactive::optimize  — logical → optimized (lookup keys + verify filter)
//!
//! 2. Subscribe (once per client)
//!    let sub_id = registry.subscribe(&plan, &params)?;
//!        └─ binds parameters, inserts into reverse index
//!
//! 3. Execute (on every mutation — hot path)
//!    let zset = db.execute_mut(sql)?;
//!    let affected = reactive::execute::on_zset(&registry, &zset);
//!        └─ execute::candidates::collect — O(1) reverse-index lookup
//!        └─ execute::verify::check       — evaluate verify_filter per candidate
//!
//! 4. Cleanup
//!    registry.unsubscribe(sub_id);
//! ```

pub mod execute;
pub mod identity;
pub mod registry;

pub use identity::{HandleRegistry, SubscriptionHandle, SubscriptionId, SubscriptionKey};
