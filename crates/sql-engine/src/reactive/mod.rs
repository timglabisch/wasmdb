//! Reactive subscription runtime (execution + registry).
//!
//! The planning side lives in [`crate::planner::reactive`]; this module owns
//! the runtime-only pieces (subscription registry + per-mutation execution).
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
pub mod registry;
