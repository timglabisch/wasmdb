//! Client-side access to fetchers.
//!
//! `fetch::<F>(url, params)` — snapshot, one-shot HTTP POST, Borsh in
//! and out. Typed wrappers and `#[wasm_bindgen]` bindings are emitted
//! by `tables-codegen` from the storage crate's `#[query]` fns.

use tables::Fetcher;

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

        let status = resp.status();
        let buf = JsFuture::from(
            resp.array_buffer().map_err(|e| FetchError::Http(format!("{e:?}")))?
        ).await.map_err(|e| FetchError::Http(format!("{e:?}")))?;
        let bytes = Uint8Array::new(&buf).to_vec();

        if !resp.ok() {
            let body = String::from_utf8_lossy(&bytes);
            let body = body.trim();
            return Err(FetchError::Http(if body.is_empty() {
                format!("HTTP {status}")
            } else {
                format!("HTTP {status}: {body}")
            }));
        }
        Ok(bytes)
    }
}

