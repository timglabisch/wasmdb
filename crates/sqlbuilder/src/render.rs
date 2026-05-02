use std::collections::HashMap;
use std::fmt;

use crate::value::{SqlPart, Value};
use crate::SqlStmt;

/// Output of [`SqlStmt::render`]: a fully-resolved SQL string and the flat
/// bind list a backend can execute against.
#[derive(Debug, Clone)]
pub struct RenderedSql {
    pub sql: String,
    pub params: Vec<(String, Value)>,
}

#[derive(Debug)]
pub enum RenderError {
    /// `{name}` appears in the SQL but no matching binding was provided.
    MissingBinding { name: String },
    /// Unbalanced `{` or `}` — use `{{` / `}}` to emit literal braces.
    UnbalancedBraces { detail: String },
}

impl fmt::Display for RenderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RenderError::MissingBinding { name } => {
                write!(f, "missing binding for placeholder {{{name}}}")
            }
            RenderError::UnbalancedBraces { detail } => write!(f, "{detail}"),
        }
    }
}

impl std::error::Error for RenderError {}

pub(crate) fn render(stmt: SqlStmt) -> Result<RenderedSql, RenderError> {
    let mut state = RenderState::default();
    let parts: HashMap<String, SqlPart> = stmt.parts.into_iter().collect();
    let sql = state.render_into(&stmt.sql, &parts, "")?;
    Ok(RenderedSql { sql, params: state.params })
}

#[derive(Default)]
struct RenderState {
    params: Vec<(String, Value)>,
    /// Stable map from a (rename-prefix, original-name) to the final flat name
    /// already emitted. Lets the same placeholder used multiple times in one
    /// fragment share a single bind entry.
    seen: HashMap<(String, String), String>,
    /// Counter for generating unique fragment-rename prefixes.
    fragment_counter: usize,
}

impl RenderState {
    fn render_into(
        &mut self,
        sql: &str,
        parts: &HashMap<String, SqlPart>,
        prefix: &str,
    ) -> Result<String, RenderError> {
        let mut out = String::with_capacity(sql.len());
        let bytes = sql.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            let b = bytes[i];
            if b == b'\'' {
                // Single-quoted string literal: copy verbatim, '' escapes a
                // single quote.
                out.push('\'');
                i += 1;
                while i < bytes.len() {
                    let c = bytes[i];
                    if c == b'\'' {
                        if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                            out.push_str("''");
                            i += 2;
                            continue;
                        }
                        out.push('\'');
                        i += 1;
                        break;
                    }
                    out.push(c as char);
                    i += 1;
                }
                continue;
            }
            if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
                // Line comment: copy through end-of-line.
                while i < bytes.len() && bytes[i] != b'\n' {
                    out.push(bytes[i] as char);
                    i += 1;
                }
                continue;
            }
            if b == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
                // Block comment: copy through `*/`.
                out.push_str("/*");
                i += 2;
                while i < bytes.len() {
                    if bytes[i] == b'*' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
                        out.push_str("*/");
                        i += 2;
                        break;
                    }
                    out.push(bytes[i] as char);
                    i += 1;
                }
                continue;
            }
            if b == b'{' {
                // `{{` → literal `{`
                if i + 1 < bytes.len() && bytes[i + 1] == b'{' {
                    out.push('{');
                    i += 2;
                    continue;
                }
                // `{name}` placeholder
                let name_start = i + 1;
                if name_start >= bytes.len() || !is_ident_start(bytes[name_start]) {
                    return Err(RenderError::UnbalancedBraces {
                        detail: format!(
                            "expected identifier after `{{` at byte {i}; use `{{{{` for a literal brace"
                        ),
                    });
                }
                let mut end = name_start;
                while end < bytes.len() && is_ident_cont(bytes[end]) {
                    end += 1;
                }
                if end >= bytes.len() || bytes[end] != b'}' {
                    return Err(RenderError::UnbalancedBraces {
                        detail: format!("missing `}}` to close placeholder starting at byte {i}"),
                    });
                }
                let name = &sql[name_start..end];
                let part = parts.get(name).ok_or_else(|| RenderError::MissingBinding {
                    name: name.to_string(),
                })?;
                match part {
                    SqlPart::Value(v) => {
                        let key = (prefix.to_string(), name.to_string());
                        let bound_name = if let Some(existing) = self.seen.get(&key) {
                            existing.clone()
                        } else {
                            let bound = if prefix.is_empty() {
                                name.to_string()
                            } else {
                                format!("{prefix}{name}")
                            };
                            self.params.push((bound.clone(), v.clone()));
                            self.seen.insert(key, bound.clone());
                            bound
                        };
                        out.push(':');
                        out.push_str(&bound_name);
                    }
                    SqlPart::Fragment(inner) => {
                        let frag_idx = self.fragment_counter;
                        self.fragment_counter += 1;
                        let inner_prefix = format!("{prefix}f{frag_idx}_");
                        let inner_parts: HashMap<String, SqlPart> =
                            inner.parts.iter().cloned().collect();
                        let rendered = self.render_into(&inner.sql, &inner_parts, &inner_prefix)?;
                        out.push('(');
                        out.push_str(&rendered);
                        out.push(')');
                    }
                }
                i = end + 1;
                continue;
            }
            if b == b'}' {
                // `}}` → literal `}`. A bare `}` is unbalanced.
                if i + 1 < bytes.len() && bytes[i + 1] == b'}' {
                    out.push('}');
                    i += 2;
                    continue;
                }
                return Err(RenderError::UnbalancedBraces {
                    detail: format!(
                        "unexpected `}}` at byte {i}; use `}}}}` for a literal brace"
                    ),
                });
            }
            out.push(b as char);
            i += 1;
        }
        Ok(out)
    }
}

fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}

fn is_ident_cont(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::IntoSqlPart;

    fn stmt(sql: &str, parts: Vec<(&str, SqlPart)>) -> SqlStmt {
        SqlStmt {
            sql: sql.to_string(),
            parts: parts.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
        }
    }

    #[test]
    fn scalar_placeholder() {
        let s = stmt("SELECT * FROM x WHERE id = {id}", vec![("id", 7_i64.into_sql_part())]);
        let r = s.render().unwrap();
        assert_eq!(r.sql, "SELECT * FROM x WHERE id = :id");
        assert_eq!(r.params.len(), 1);
        assert_eq!(r.params[0].0, "id");
    }

    #[test]
    fn placeholder_inside_string_literal_is_ignored() {
        let s = stmt("SELECT '{not_a_param}', {real}", vec![("real", 1_i64.into_sql_part())]);
        let r = s.render().unwrap();
        assert_eq!(r.sql, "SELECT '{not_a_param}', :real");
        assert_eq!(r.params.len(), 1);
    }

    #[test]
    fn raw_engine_colon_placeholders_pass_through_untouched() {
        // Bare `:foo` is engine syntax — the renderer must not touch it.
        let s = stmt(
            "SELECT :raw_engine_param, {real}",
            vec![("real", 1_i64.into_sql_part())],
        );
        let r = s.render().unwrap();
        assert_eq!(r.sql, "SELECT :raw_engine_param, :real");
        assert_eq!(r.params.len(), 1);
    }

    #[test]
    fn same_placeholder_multiple_uses_one_bind() {
        let s = stmt(
            "SELECT * FROM x WHERE a = {id} OR b = {id}",
            vec![("id", 7_i64.into_sql_part())],
        );
        let r = s.render().unwrap();
        assert_eq!(r.sql, "SELECT * FROM x WHERE a = :id OR b = :id");
        assert_eq!(r.params.len(), 1);
    }

    #[test]
    fn fragment_splice_with_rename() {
        let inner = stmt("status = {status}", vec![("status", "active".into_sql_part())]);
        let outer = stmt(
            "SELECT * FROM x WHERE {cond} AND tid = {tid}",
            vec![
                ("cond", SqlPart::Fragment(inner)),
                ("tid", 7_i64.into_sql_part()),
            ],
        );
        let r = outer.render().unwrap();
        assert_eq!(r.sql, "SELECT * FROM x WHERE (status = :f0_status) AND tid = :tid");
        let names: Vec<&str> = r.params.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(names, vec!["f0_status", "tid"]);
    }

    #[test]
    fn missing_binding_errors() {
        let s = stmt("SELECT {missing}", vec![]);
        let err = s.render().unwrap_err();
        assert!(matches!(err, RenderError::MissingBinding { .. }));
    }

    #[test]
    fn line_comment_preserves_placeholder_text() {
        let s = stmt("-- {commented}\nSELECT {real}", vec![("real", 1_i64.into_sql_part())]);
        let r = s.render().unwrap();
        assert!(r.sql.contains("-- {commented}"));
        assert_eq!(r.params.len(), 1);
    }

    #[test]
    fn doubled_braces_render_as_literals() {
        let s = stmt("SELECT '{{not_a_param}}', {real}", vec![("real", 1_i64.into_sql_part())]);
        // Inside the string literal `{{` and `}}` are passed through verbatim.
        // Outside, they would collapse to `{` / `}`.
        let r = s.render().unwrap();
        assert!(r.sql.contains("{{not_a_param}}"));

        let s2 = stmt("SELECT {{literal_brace}}", vec![]);
        let r2 = s2.render().unwrap();
        assert_eq!(r2.sql, "SELECT {literal_brace}");
    }

    #[test]
    fn unbalanced_brace_errors() {
        let s = stmt("SELECT {unterminated", vec![("unterminated", 1_i64.into_sql_part())]);
        assert!(matches!(s.render().unwrap_err(), RenderError::UnbalancedBraces { .. }));
    }
}
