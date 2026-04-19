//! Client-side access to fetchers.
//!
//! `fetch::<F>(url, params)` — snapshot, one-shot HTTP POST, Borsh in
//! and out. `params` is just `F` itself (the user's `#[fetcher]` struct
//! is its own params).

use tables::Fetcher;

/// `#[row]` / `#[fetcher]` macros — see `tables-macros`.
pub use tables_macros::{fetcher, row};

#[derive(Debug)]
pub enum FetchError {
    Encode(String),
    Decode(String),
    Http(String),
}

impl std::fmt::Display for FetchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FetchError::Encode(s) => write!(f, "encode: {s}"),
            FetchError::Decode(s) => write!(f, "decode: {s}"),
            FetchError::Http(s)   => write!(f, "http: {s}"),
        }
    }
}

impl std::error::Error for FetchError {}

/// Snapshot fetch. POSTs a Borsh-encoded `FetchRequest` to `url`,
/// deserializes the response body as `Vec<F::Row>`. Runs against the
/// browser's `fetch` API — wasm target only at runtime.
pub async fn fetch<F: Fetcher>(url: &str, params: F::Params) -> Result<Vec<F::Row>, FetchError> {
    let params_bytes = borsh::to_vec(&params)
        .map_err(|e| FetchError::Encode(e.to_string()))?;
    let request = tables::FetchRequest { fetcher_id: F::ID.to_string(), params: params_bytes };
    let body = borsh::to_vec(&request)
        .map_err(|e| FetchError::Encode(e.to_string()))?;
    let response = wasm_http::post_bytes(url, &body).await?;
    borsh::from_slice(&response).map_err(|e| FetchError::Decode(e.to_string()))
}

mod wasm_http {
    use super::FetchError;
    use js_sys::Uint8Array;
    use wasm_bindgen::JsCast;
    use wasm_bindgen_futures::JsFuture;

    pub async fn post_bytes(url: &str, body: &[u8]) -> Result<Vec<u8>, FetchError> {
        let opts = web_sys::RequestInit::new();
        opts.set_method("POST");
        let uint8_body = Uint8Array::from(body);
        opts.set_body(&uint8_body);

        let request = web_sys::Request::new_with_str_and_init(url, &opts)
            .map_err(|e| FetchError::Http(format!("{e:?}")))?;
        request.headers().set("Content-Type", "application/octet-stream")
            .map_err(|e| FetchError::Http(format!("{e:?}")))?;

        let window = web_sys::window()
            .ok_or_else(|| FetchError::Http("no global window".into()))?;
        let resp_value = JsFuture::from(window.fetch_with_request(&request)).await
            .map_err(|e| FetchError::Http(format!("{e:?}")))?;
        let resp: web_sys::Response = resp_value.dyn_into()
            .map_err(|e| FetchError::Http(format!("{e:?}")))?;

        if !resp.ok() {
            return Err(FetchError::Http(format!("HTTP {}", resp.status())));
        }

        let buf = JsFuture::from(
            resp.array_buffer().map_err(|e| FetchError::Http(format!("{e:?}")))?
        ).await.map_err(|e| FetchError::Http(format!("{e:?}")))?;
        Ok(Uint8Array::new(&buf).to_vec())
    }
}

// Re-exports for the `wasm_fetch!` macro. Users only need `tables-client`
// in their Cargo.toml; the macro reaches everything through here.
#[doc(hidden)]
pub mod __rt {
    pub use ::serde_wasm_bindgen;
    pub use ::tables;
    pub use ::wasm_bindgen;
}

/// Generates a `#[wasm_bindgen]` async fn that fetches via `F` and
/// returns rows as a JS value. JS calls it with a plain object for
/// params; the macro takes care of serde/borsh conversion.
///
/// ```ignore
/// tables_client::wasm_fetch!(fetch_customers_by_owner, ByOwner, "/table-fetch");
/// ```
#[macro_export]
macro_rules! wasm_fetch {
    ($fn_name:ident, $fetcher:ty, $url:expr) => {
        #[$crate::__rt::wasm_bindgen::prelude::wasm_bindgen]
        pub async fn $fn_name(
            params: $crate::__rt::wasm_bindgen::JsValue,
        ) -> ::core::result::Result<
            $crate::__rt::wasm_bindgen::JsValue,
            $crate::__rt::wasm_bindgen::JsError,
        > {
            let params: <$fetcher as $crate::__rt::tables::Fetcher>::Params =
                $crate::__rt::serde_wasm_bindgen::from_value(params)
                    .map_err(|e| $crate::__rt::wasm_bindgen::JsError::new(&e.to_string()))?;
            let rows = $crate::fetch::<$fetcher>($url, params)
                .await
                .map_err(|e| $crate::__rt::wasm_bindgen::JsError::new(&e.to_string()))?;
            $crate::__rt::serde_wasm_bindgen::to_value(&rows)
                .map_err(|e| $crate::__rt::wasm_bindgen::JsError::new(&e.to_string()))
        }
    };
}
