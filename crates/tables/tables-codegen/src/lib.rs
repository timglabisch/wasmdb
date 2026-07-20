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
    source_roots: Vec<PathBuf>,
    out_file: String,
    mode: Mode,
    url: String,
    wasm_bindings: bool,
    ctx_ty: Option<String>,
    ts_requirements_out: Option<PathBuf>,
    ts_rows_out: Option<PathBuf>,
    projections_path: String,
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
            source_roots: Vec::new(),
            out_file: "generated.rs".into(),
            mode: Mode::Client,
            url: "/table-fetch".into(),
            wasm_bindings: false,
            ctx_ty: None,
            ts_requirements_out: None,
            ts_rows_out: None,
            projections_path: "crate".into(),
        }
    }

    /// Path to a source crate's `src/` (where `lib.rs` lives). **Additiv:**
    /// mehrfaches Aufrufen scannt MEHRERE Crates in EIN Modell (die Roots werden
    /// gemergt, nicht überschrieben) — nötig, seit `#[row]`-Deklarationen über
    /// zwei Crates verteilt liegen (z.B. Domain-Stammdaten + Beleg-Read-Model).
    pub fn source_root(mut self, path: impl AsRef<Path>) -> Self {
        self.source_roots.push(path.as_ref().to_path_buf());
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

    /// Client-mode only: also emit a TypeScript file describing the typed
    /// `requirements.<module>.<query>(...)` builders. The path is relative
    /// to the build.rs CWD (typically the consumer crate's root).
    pub fn ts_requirements_out(mut self, path: impl AsRef<Path>) -> Self {
        self.ts_requirements_out = Some(path.as_ref().to_path_buf());
        self
    }

    /// Client-mode only: emit one TypeScript file per `#[row]` that declares
    /// `#[export(...)]` attributes into the given directory. Each row owns
    /// its export list — there is no global default. Rows without exports
    /// are skipped.
    pub fn ts_rows_out(mut self, dir: impl AsRef<Path>) -> Self {
        self.ts_rows_out = Some(dir.as_ref().to_path_buf());
        self
    }

    /// Client-mode only: path prefix under which the scanned crate's
    /// `#[projection]` types are reachable FROM the including crate —
    /// `register_all_projections` references them (function bodies can't
    /// be re-emitted like rows). Default `"crate"` fits scanning the own
    /// crate; when the generated file lands in a different crate (the
    /// typical wasm build), pass the domain crate's name, e.g.
    /// `"::invoice_demo_domain"`.
    pub fn projections_path(mut self, path: impl Into<String>) -> Self {
        self.projections_path = path.into();
        self
    }

    pub fn compile(self) -> Result<(), CodegenError> {
        let (first, rest) = self
            .source_roots
            .split_first()
            .ok_or(CodegenError::MissingSourceRoot)?;
        // Ein Modell über ALLE Roots: den ersten scannen, jeden weiteren
        // hineinmergen. Rows/Queries verschiedener Crates leben in disjunkten
        // Modulpfaden; `emit` baut daraus den Modulbaum und EIN
        // `register_all_tables`. Ohne diesen Merge gewönne der letzte
        // `source_root`-Aufruf und die Tabellen der übrigen Crates verschwänden
        // still aus der Registrierung.
        let mut model = parse::scan(first)?;
        for root in rest {
            let m = parse::scan(root)?;
            model.modules.extend(m.modules);
            model.scanned_files.extend(m.scanned_files);
        }

        for path in &model.scanned_files {
            println!("cargo::rerun-if-changed={}", path.display());
        }

        let out_dir = std::env::var_os("OUT_DIR").ok_or(CodegenError::NoOutDir)?;
        let out_path = PathBuf::from(out_dir).join(&self.out_file);

        let tokens = match self.mode {
            Mode::Client => emit::emit_client(
                &model,
                &self.url,
                self.wasm_bindings,
                &self.projections_path,
            )?,
            Mode::Server => {
                let ctx = self.ctx_ty.as_deref().unwrap_or("crate::AppCtx");
                emit::emit_server(&model, ctx)?
            }
        };

        let file: syn::File =
            syn::parse2(tokens).map_err(|e| CodegenError::Emit(e.to_string()))?;
        let formatted = prettyplease::unparse(&file);
        std::fs::write(&out_path, formatted)?;

        if let (Mode::Client, Some(ts_path)) = (self.mode, self.ts_requirements_out.as_ref()) {
            let ts = emit::emit_ts_requirements(&model);
            if let Some(parent) = ts_path.parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent)?;
                }
            }
            std::fs::write(ts_path, ts)?;
        }

        if let (Mode::Client, Some(dir)) = (self.mode, self.ts_rows_out.as_ref()) {
            std::fs::create_dir_all(dir)?;
            for (file, body) in emit::emit_ts_rows(&model)? {
                std::fs::write(dir.join(file), body)?;
            }
        }
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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    fn fixture_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixture")
    }

    #[test]
    fn scan_collects_projection_impls() {
        let model = crate::parse::scan(&fixture_root()).unwrap();
        let projections: Vec<&str> = model
            .modules
            .iter()
            .flat_map(|m| m.projections.iter().map(|p| p.name.as_str()))
            .collect();
        // The #[projection_row] struct (the log) must NOT land here —
        // it is a row, not a registrable projection.
        assert_eq!(projections, vec!["DraftFoldView"]);
    }

    /// `#[projection_row]` scans as a row with the generated bookkeeping
    /// columns appended, exactly mirroring the macro expansion.
    #[test]
    fn scan_expands_projection_row_to_full_row() {
        let model = crate::parse::scan(&fixture_root()).unwrap();
        let log = model
            .modules
            .iter()
            .flat_map(|m| m.rows.iter())
            .find(|r| r.name == "DraftLog")
            .expect("DraftLog scanned as row");
        let field_names: Vec<&str> = log.fields.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(
            field_names,
            vec!["command_id", "doc_id", "client_parent_id", "server_parent_id", "payload"]
        );
        assert_eq!(log.pk_name, "command_id");
        assert!(log.exports.is_empty());
    }

    #[test]
    fn client_emit_duplicates_projection_log_with_generated_columns() {
        let model = crate::parse::scan(&fixture_root()).unwrap();
        let tokens = crate::emit::emit_client(&model, "/table-fetch", false, "crate")
            .unwrap()
            .to_string();
        assert!(tokens.contains("struct DraftLog"), "{tokens}");
        assert!(tokens.contains("server_parent_id"), "{tokens}");
        assert!(tokens.contains("payload"), "{tokens}");
    }

    #[test]
    fn client_emit_registers_projections_under_the_given_path() {
        let model = crate::parse::scan(&fixture_root()).unwrap();
        let tokens = crate::emit::emit_client(&model, "/table-fetch", false, "::my_domain")
            .unwrap()
            .to_string();
        assert!(tokens.contains("register_all_projections"), "{tokens}");
        assert!(
            tokens.contains(":: my_domain :: DraftFoldView"),
            "projection referenced in its source crate: {tokens}"
        );
        // Rows are still re-emitted as usual.
        assert!(tokens.contains("struct DraftEvent"), "{tokens}");
    }

    #[test]
    fn client_emit_without_projections_emits_no_register_fn() {
        let mut model = crate::parse::scan(&fixture_root()).unwrap();
        for m in &mut model.modules {
            m.projections.clear();
        }
        let tokens = crate::emit::emit_client(&model, "/table-fetch", false, "crate")
            .unwrap()
            .to_string();
        assert!(
            !tokens.contains("register_all_projections"),
            "no projections → no fn (and no database-projection dep): {tokens}"
        );
    }
}
