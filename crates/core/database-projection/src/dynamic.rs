//! Demand-driven projection instances (design doc §12).
//!
//! A [`DynamicProjection`] is a template; an **instance** is (template id,
//! compound name). Instances are activated and deactivated at runtime —
//! materialized while observed, evicted at refcount 0. The instance's
//! footprint (equality bindings between source columns and name components)
//! compiles to the same [`OptimizedReactiveCondition`] structure that query
//! subscriptions use, so the engine can route mutations to affected
//! instances through the shared candidates→verify machinery without any
//! change to `sql-engine`.

use sql_engine::planner::reactive::{
    OptimizedReactiveCondition, ReactiveLookupKey, ReactiveLookupStrategy,
};
use sql_engine::planner::shared::plan::{ColumnRef, PlanFilterPredicate};
use sql_engine::storage::CellValue;
use sql_parser::ast::Value;

use crate::spec::{FoldCache, Inputs, OutputRow, ReadCtx};

/// The compound unique name of one instance — a single composite
/// identifier (e.g. `[Str("account"), Str("carol")]`), not a list of
/// independent slices.
pub type InstanceName = Vec<CellValue>;

/// One source table of a dynamic template with its equality bindings:
/// `(column index, name component index)`. A row belongs to the instance
/// iff `row[col] == name[comp]` for every binding.
#[derive(Debug, Clone)]
pub struct FootprintSource {
    pub table: String,
    pub bind: Vec<(usize, usize)>,
}

/// Static description of a dynamic template, produced once at registration.
#[derive(Debug, Clone)]
pub struct DynamicSpec {
    /// Unique id across static AND dynamic projections.
    pub id: String,
    pub sources: Vec<FootprintSource>,
    /// Read-only render inputs; any change re-renders ALL active instances
    /// of the template (coarse by design, like the static path — v1).
    pub reads: Vec<String>,
    /// Output tables, owned exclusively by this template. Dynamic outputs
    /// are DAG leaves: nothing may consume them (v1).
    pub outputs: Vec<String>,
}

/// A materialized view template whose instances are activated on demand.
///
/// `project` has the same contract as [`crate::Projection::project`]:
/// PURE, called with the current rows of every footprint source for the
/// instance, its return value fully replaces the previous render. Unlike
/// the static path it IS called (and stays active) when the sources hold
/// zero rows for the instance — demand is the lifecycle, not data presence.
pub trait DynamicProjection {
    fn spec(&self) -> DynamicSpec;

    fn project(
        &self,
        name: &[CellValue],
        inputs: &Inputs,
        ctx: &ReadCtx<'_>,
        cache: &mut FoldCache,
    ) -> Result<Vec<OutputRow>, String>;
}

/// `CellValue` → `ast::Value` for condition construction. Total: every
/// name component has a value form (no placeholders involved).
pub fn cell_to_value(cell: &CellValue) -> Value {
    match cell {
        CellValue::I64(n) => Value::Int(*n),
        CellValue::Str(s) => Value::Text(s.clone()),
        CellValue::Uuid(b) => Value::Uuid(*b),
        CellValue::Null => Value::Null,
    }
}

/// Compile an instance's footprint to reactive conditions: one condition
/// per source, an `IndexLookup` with ONE composite key set from all
/// bindings, and a verify AND-chain of `Equals`. A source without bindings
/// degenerates to `TableScan` (every mutation of the table affects the
/// instance).
///
/// Returns an error if a binding references a name component that the
/// given name does not have.
pub fn compile_footprint(
    spec: &DynamicSpec,
    name: &[CellValue],
) -> Result<Vec<OptimizedReactiveCondition>, String> {
    let mut conditions = Vec::with_capacity(spec.sources.len());
    for (fp_idx, source) in spec.sources.iter().enumerate() {
        for &(_, comp) in &source.bind {
            if comp >= name.len() {
                return Err(format!(
                    "template '{}': binding references name component {comp}, \
                     but name '{}' has only {} components",
                    spec.id,
                    display_name(name),
                    name.len()
                ));
            }
        }
        let (strategy, verify_filter) = if source.bind.is_empty() {
            (ReactiveLookupStrategy::TableScan, PlanFilterPredicate::None)
        } else {
            let keys: Vec<ReactiveLookupKey> = source
                .bind
                .iter()
                .map(|&(col, comp)| ReactiveLookupKey { col, value: cell_to_value(&name[comp]) })
                .collect();
            let verify = PlanFilterPredicate::combine_and(source.bind.iter().map(|&(col, comp)| {
                PlanFilterPredicate::Equals {
                    col: ColumnRef { source: fp_idx, col },
                    value: cell_to_value(&name[comp]),
                }
            }));
            (ReactiveLookupStrategy::IndexLookup { lookup_key_sets: vec![keys] }, verify)
        };
        conditions.push(OptimizedReactiveCondition {
            table: source.table.clone(),
            source_idx: fp_idx,
            strategy,
            verify_filter,
        });
    }
    Ok(conditions)
}

/// Display form of a compound name for errors and events: components
/// joined with `/`.
pub fn display_name(name: &[CellValue]) -> String {
    name.iter().map(display_component).collect::<Vec<_>>().join("/")
}

fn display_component(c: &CellValue) -> String {
    match c {
        CellValue::I64(v) => v.to_string(),
        CellValue::Str(s) => s.clone(),
        CellValue::Uuid(b) => crate::engine::format_uuid(b),
        CellValue::Null => "NULL".to_string(),
    }
}
