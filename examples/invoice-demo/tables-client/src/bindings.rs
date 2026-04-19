//! JS entry points — one per table kind. wasm-bindgen can't cross the
//! JS boundary with generics, so each table needs a concrete wrapper.

use crate::customers::{Customers, CustomersParams};
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub async fn fetch_customers(owner_id: f64) -> Result<JsValue, JsError> {
    let params = CustomersParams { owner_id: owner_id as i64 };
    let rows = crate::fetch::<Customers>(params)
        .await
        .map_err(|e| JsError::new(&e.to_string()))?;
    serde_wasm_bindgen::to_value(&rows).map_err(|e| JsError::new(&e.to_string()))
}
