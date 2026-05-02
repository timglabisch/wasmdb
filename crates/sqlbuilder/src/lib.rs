//! Composable SQL fragments with named placeholders.
//!
//! Build statements with the [`sql!`] macro; assemble lists with [`sql_batch!`].
//! Statements are pure data ([`SqlStmt`]) — composition splices fragments
//! at render time, with placeholder collisions resolved automatically.
//!
//! Macro-level placeholders use **`{name}`** (format!-style). They are
//! distinct from any `:name` placeholders the underlying engine may parse —
//! `:name` passes through untouched. The renderer translates `{name}` into
//! the engine's `:name` form in the final SQL.
//!
//! # Example
//!
//! ```ignore
//! use sqlbuilder::{sql, SqlStmt};
//!
//! let cond = sql!("status = {status}", status = "active");
//! let stmt = sql!("SELECT * FROM x WHERE {cond} AND tid = {tid}",
//!                 cond = cond,
//!                 tid = 7_i64);
//! let rendered = stmt.render();
//! ```
//!
//! Backends consume [`RenderedSql`]: a flat SQL string + flat bind list.

pub use sqlbuilder_macros::{sql, sql_batch};

mod render;
mod value;

pub use render::{RenderedSql, RenderError};
pub use value::{IntoSqlPart, SqlPart, Value};

/// A SQL statement with named placeholders and the values (or sub-fragments)
/// bound to those names. Construct via the [`sql!`] macro.
#[derive(Debug, Clone)]
pub struct SqlStmt {
    pub sql: String,
    pub parts: Vec<(String, SqlPart)>,
}

impl SqlStmt {
    /// Construct directly. Most callers should use the [`sql!`] macro instead.
    pub fn new(sql: impl Into<String>, parts: Vec<(String, SqlPart)>) -> Self {
        Self { sql: sql.into(), parts }
    }

    /// Flatten fragments and produce a single SQL string + bind list.
    pub fn render(self) -> Result<RenderedSql, RenderError> {
        render::render(self)
    }
}
