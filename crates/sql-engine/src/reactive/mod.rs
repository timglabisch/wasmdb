//! Reactive subscription system.
//!
//! ## Flow
//!
//! ```text
//! 1. Plan (once per query)
//!    let plan = reactive::plan_reactive(ast, &schemas)?;
//!        └─ plan::extract  — AST → logical conditions
//!        └─ plan::optimize — logical → optimized (lookup keys + verify filter)
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

pub mod plan;
pub mod execute;
pub mod registry;

use std::collections::HashMap;

use sql_parser::ast;
use crate::planner::PlanError;
use crate::schema::TableSchema;

/// Entry-point: extract and optimize reactive conditions from an AstSelect.
///
/// Pipeline: plan_select_ctx() → extract conditions → optimize (extract lookup keys).
/// Returns a `ReactivePlan` that bundles the optimized conditions with the
/// source schemas (for pretty-printing and inspection).
pub fn plan_reactive(
    ast: &ast::AstSelect,
    table_schemas: &HashMap<String, TableSchema>,
) -> Result<plan::ReactivePlan, PlanError> {
    let mut ctx = crate::planner::make_plan_context(table_schemas);
    let main = crate::planner::plan_select_ctx(ast, &mut ctx)?;
    let logical = plan::extract::extract_reactive_conditions(ast, &main)?;
    let conditions = plan::optimize::optimize(logical);
    let sources = main.sources;
    Ok(plan::ReactivePlan { conditions, sources })
}
