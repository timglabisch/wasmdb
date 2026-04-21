//! Codegen for the `tables` RPC ecosystem.
//!
//! Reads a source tree with `#[row]`-annotated structs and
//! `#[query]`-annotated async fns; emits one of two outputs:
//!
//! - **Client mode**: Row duplicates + `impl Row`, params structs +
//!   `impl Fetcher`, and `pub async fn` call wrappers against a
//!   configured URL. Optionally `#[wasm_bindgen]` bindings.
//! - **Server mode**: params structs + `impl Fetcher` and
//!   `pub fn register_{fn}` glue that wires the user's async fn into
//!   a `tables_storage::Registry<AppCtx>`.
//!
//! Output goes to `$OUT_DIR/<file>`. Include via
//! `include!(concat!(env!("OUT_DIR"), "/generated.rs"))`.

mod emit;
mod parse;

use std::io;
use std::path::{Path, PathBuf};

pub struct Builder {
    source_root: Option<PathBuf>,
    out_file: String,
    mode: Mode,
    url: String,
    wasm_bindings: bool,
    ctx_ty: Option<String>,
}

#[derive(Clone, Copy)]
enum Mode {
    Client,
    Server,
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}

impl Builder {
    pub fn new() -> Self {
        Self {
            source_root: None,
            out_file: "generated.rs".into(),
            mode: Mode::Client,
            url: "/table-fetch".into(),
            wasm_bindings: false,
            ctx_ty: None,
        }
    }

    /// Path to the source crate's `src/` (where `lib.rs` lives).
    pub fn source_root(mut self, path: impl AsRef<Path>) -> Self {
        self.source_root = Some(path.as_ref().to_path_buf());
        self
    }

    pub fn out_file(mut self, name: impl Into<String>) -> Self {
        self.out_file = name.into();
        self
    }

    pub fn client(mut self) -> Self {
        self.mode = Mode::Client;
        self
    }

    pub fn server(mut self) -> Self {
        self.mode = Mode::Server;
        self
    }

    /// Endpoint path for the client-side fetch call (default `/table-fetch`).
    pub fn url(mut self, path: impl Into<String>) -> Self {
        self.url = path.into();
        self
    }

    /// Emit `#[wasm_bindgen]`-wrapped entry points (client mode only).
    pub fn wasm_bindings(mut self, on: bool) -> Self {
        self.wasm_bindings = on;
        self
    }

    /// Server-mode only: the app-ctx type path the `register_*` fns
    /// register against (default `crate::AppCtx`).
    pub fn ctx_type(mut self, ty: impl Into<String>) -> Self {
        self.ctx_ty = Some(ty.into());
        self
    }

    pub fn compile(self) -> Result<(), CodegenError> {
        let root = self
            .source_root
            .as_ref()
            .ok_or(CodegenError::MissingSourceRoot)?;
        let model = parse::scan(root)?;

        for path in &model.scanned_files {
            println!("cargo::rerun-if-changed={}", path.display());
        }

        let out_dir = std::env::var_os("OUT_DIR").ok_or(CodegenError::NoOutDir)?;
        let out_path = PathBuf::from(out_dir).join(&self.out_file);

        let tokens = match self.mode {
            Mode::Client => emit::emit_client(&model, &self.url, self.wasm_bindings)?,
            Mode::Server => {
                let ctx = self.ctx_ty.as_deref().unwrap_or("crate::AppCtx");
                emit::emit_server(&model, ctx)?
            }
        };

        let file: syn::File =
            syn::parse2(tokens).map_err(|e| CodegenError::Emit(e.to_string()))?;
        let formatted = prettyplease::unparse(&file);
        std::fs::write(&out_path, formatted)?;
        Ok(())
    }
}

#[derive(Debug)]
pub enum CodegenError {
    MissingSourceRoot,
    NoOutDir,
    Io(io::Error),
    Parse(String),
    Emit(String),
}

impl std::fmt::Display for CodegenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingSourceRoot => write!(f, "Builder::source_root is required"),
            Self::NoOutDir => write!(f, "OUT_DIR not set (is this running from build.rs?)"),
            Self::Io(e) => write!(f, "io: {e}"),
            Self::Parse(s) => write!(f, "parse: {s}"),
            Self::Emit(s) => write!(f, "emit: {s}"),
        }
    }
}

impl std::error::Error for CodegenError {}

impl From<io::Error> for CodegenError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}
