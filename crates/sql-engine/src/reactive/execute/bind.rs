//! Parameter binding for reactive conditions: resolves Value::Placeholder.

use crate::execute::Params;
use crate::execute::ExecuteError;
use crate::execute::bind::{resolve_filter, resolve_value};
use crate::reactive::plan::{OptimizedReactiveCondition, ReactiveLookupStrategy};

/// Resolve all placeholders in optimized reactive conditions.
pub fn resolve_reactive_conditions(
    conditions: &[OptimizedReactiveCondition],
    params: &Params,
) -> Result<Vec<OptimizedReactiveCondition>, ExecuteError> {
    if params.is_empty() {
        return Ok(conditions.to_vec());
    }
    let mut resolved = conditions.to_vec();
    for cond in &mut resolved {
        if let ReactiveLookupStrategy::IndexLookup { ref mut lookup_keys } = cond.strategy {
            for key in lookup_keys.iter_mut() {
                key.value = resolve_value(&key.value, params)?;
            }
        }
        cond.verify_filter = resolve_filter(&cond.verify_filter, params)?;
    }
    Ok(resolved)
}
