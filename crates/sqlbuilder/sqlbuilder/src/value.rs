use crate::SqlStmt;

/// Backend-agnostic bind value. Mirrors the value variants supported by the
/// SQL engines this crate is intended to drive (single scalars + list params
/// for `IN (...)` clauses).
#[derive(Debug, Clone)]
pub enum Value {
    Int(i64),
    Text(String),
    Uuid([u8; 16]),
    Null,
    IntList(Vec<i64>),
    TextList(Vec<String>),
    UuidList(Vec<[u8; 16]>),
}

/// What gets bound to a `:name` placeholder: either a scalar value (becomes
/// a bind-param) or a nested `SqlStmt` (spliced inline at render time).
#[derive(Debug, Clone)]
pub enum SqlPart {
    Value(Value),
    Fragment(SqlStmt),
}

/// Convert a Rust value into a [`SqlPart`]. Implemented for the common
/// primitive types plus `SqlStmt` itself (which becomes a `Fragment`).
///
/// Method takes `&self` so call sites can pass owned or borrowed values
/// without consuming them — Rust auto-deref handles `&T` / `&&T` automatically.
pub trait IntoSqlPart {
    fn into_sql_part(&self) -> SqlPart;
}

// ── Scalars ──────────────────────────────────────────────────────────────

impl IntoSqlPart for i64 {
    fn into_sql_part(&self) -> SqlPart {
        SqlPart::Value(Value::Int(*self))
    }
}

impl IntoSqlPart for i32 {
    fn into_sql_part(&self) -> SqlPart {
        SqlPart::Value(Value::Int(*self as i64))
    }
}

impl IntoSqlPart for u32 {
    fn into_sql_part(&self) -> SqlPart {
        SqlPart::Value(Value::Int(*self as i64))
    }
}

impl IntoSqlPart for bool {
    fn into_sql_part(&self) -> SqlPart {
        SqlPart::Value(Value::Int(if *self { 1 } else { 0 }))
    }
}

impl IntoSqlPart for String {
    fn into_sql_part(&self) -> SqlPart {
        SqlPart::Value(Value::Text(self.clone()))
    }
}

impl IntoSqlPart for str {
    fn into_sql_part(&self) -> SqlPart {
        SqlPart::Value(Value::Text(self.to_string()))
    }
}

impl IntoSqlPart for [u8; 16] {
    fn into_sql_part(&self) -> SqlPart {
        SqlPart::Value(Value::Uuid(*self))
    }
}

// ── Option<T> → Null on None ─────────────────────────────────────────────

impl<T: IntoSqlPart> IntoSqlPart for Option<T> {
    fn into_sql_part(&self) -> SqlPart {
        match self {
            Some(v) => v.into_sql_part(),
            None => SqlPart::Value(Value::Null),
        }
    }
}

// ── Lists ────────────────────────────────────────────────────────────────

impl IntoSqlPart for Vec<i64> {
    fn into_sql_part(&self) -> SqlPart {
        SqlPart::Value(Value::IntList(self.clone()))
    }
}

impl IntoSqlPart for Vec<String> {
    fn into_sql_part(&self) -> SqlPart {
        SqlPart::Value(Value::TextList(self.clone()))
    }
}

impl IntoSqlPart for Vec<[u8; 16]> {
    fn into_sql_part(&self) -> SqlPart {
        SqlPart::Value(Value::UuidList(self.clone()))
    }
}

// ── Fragments ────────────────────────────────────────────────────────────

impl IntoSqlPart for SqlStmt {
    fn into_sql_part(&self) -> SqlPart {
        SqlPart::Fragment(self.clone())
    }
}

// ── Pre-built parts (idempotent) ─────────────────────────────────────────

impl IntoSqlPart for SqlPart {
    fn into_sql_part(&self) -> SqlPart {
        self.clone()
    }
}

impl IntoSqlPart for Value {
    fn into_sql_part(&self) -> SqlPart {
        SqlPart::Value(self.clone())
    }
}

// ── Reference forwarding ──────────────────────────────────────────────────
//
// Lets call sites pass `&T` (or `&&T`, common when destructuring `&self`)
// without needing a separate impl per reference depth. The blanket subsumes
// the `&str` case via `IntoSqlPart for str`.

impl<T: ?Sized + IntoSqlPart> IntoSqlPart for &T {
    fn into_sql_part(&self) -> SqlPart {
        (**self).into_sql_part()
    }
}

// ── Optional integration: in-repo sql-engine Uuid ────────────────────────

#[cfg(feature = "sql-engine")]
impl IntoSqlPart for sql_engine::storage::Uuid {
    fn into_sql_part(&self) -> SqlPart {
        SqlPart::Value(Value::Uuid(self.0))
    }
}
