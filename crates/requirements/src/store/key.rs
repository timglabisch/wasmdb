//! Canonical construction of `RequirementKey` strings.
//!
//! For Fetched, the key encodes `name + positional args`. For Derived,
//! it encodes `sql + named params + upstream keys`. The encoding is
//! deterministic — same inputs always produce the same string — but
//! is *not* JSON. We don't parse it back; equality is the only consumer.

use std::collections::HashMap;
use std::fmt::Write;

use sql_engine::execute::ParamValue;
use sql_parser::ast::Value;

use super::slot::RequirementKey;

/// Build the canonical key for a Fetched requirement.
pub fn make_fetched_key(name: &str, args: &[Value]) -> RequirementKey {
    let mut s = String::with_capacity(32 + name.len() + 8 * args.len());
    s.push_str("fetched:");
    s.push_str(name);
    s.push(':');
    s.push('[');
    for (i, v) in args.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        write_value(&mut s, v);
    }
    s.push(']');
    RequirementKey::new(s)
}

/// Build the canonical key for a Derived requirement.
///
/// The upstream-keys list is part of identity: two `useQuery` calls with
/// the same SQL+params but different `requires` produce different slots.
/// See the design doc for why.
pub fn make_derived_key(
    sql: &str,
    params: &HashMap<String, ParamValue>,
    upstream_keys: &[RequirementKey],
) -> RequirementKey {
    let mut s = String::with_capacity(64 + sql.len());
    s.push_str("derived:");
    write_str(&mut s, sql);
    s.push('|');

    // Params: sort by key for determinism.
    let mut keys: Vec<&String> = params.keys().collect();
    keys.sort();
    s.push('{');
    for (i, k) in keys.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        write_str(&mut s, k);
        s.push(':');
        write_param(&mut s, &params[*k]);
    }
    s.push('}');
    s.push('|');

    // Upstream keys: sort for determinism — order of `requires:` shouldn't matter.
    let mut ups: Vec<&str> = upstream_keys.iter().map(|k| k.as_str()).collect();
    ups.sort();
    s.push('[');
    for (i, u) in ups.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        write_str(&mut s, u);
    }
    s.push(']');

    RequirementKey::new(s)
}

fn write_value(out: &mut String, v: &Value) {
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(true) => out.push_str("true"),
        Value::Bool(false) => out.push_str("false"),
        Value::Int(i) => write!(out, "{i}").unwrap(),
        Value::Float(f) => write!(out, "{f:?}").unwrap(),
        Value::Text(s) => write_str(out, s),
        Value::Uuid(b) => write_uuid(out, b),
        Value::Placeholder(name) => {
            out.push('$');
            out.push_str(name);
        }
    }
}

fn write_param(out: &mut String, p: &ParamValue) {
    match p {
        ParamValue::Null => out.push_str("null"),
        ParamValue::Int(i) => write!(out, "{i}").unwrap(),
        ParamValue::Text(s) => write_str(out, s),
        ParamValue::Uuid(b) => write_uuid(out, b),
        ParamValue::IntList(xs) => {
            out.push('[');
            for (i, x) in xs.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write!(out, "{x}").unwrap();
            }
            out.push(']');
        }
        ParamValue::TextList(xs) => {
            out.push('[');
            for (i, x) in xs.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_str(out, x);
            }
            out.push(']');
        }
        ParamValue::UuidList(xs) => {
            out.push('[');
            for (i, x) in xs.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_uuid(out, x);
            }
            out.push(']');
        }
    }
}

fn write_str(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => write!(out, "\\u{:04x}", c as u32).unwrap(),
            c => out.push(c),
        }
    }
    out.push('"');
}

fn write_uuid(out: &mut String, b: &[u8; 16]) {
    out.push_str("u\"");
    for byte in b {
        write!(out, "{byte:02x}").unwrap();
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fetched_key_with_int_arg() {
        let k = make_fetched_key("invoices.by_id", &[Value::Int(1)]);
        assert_eq!(k.as_str(), "fetched:invoices.by_id:[1]");
    }

    #[test]
    fn fetched_key_with_no_args() {
        let k = make_fetched_key("invoices.all", &[]);
        assert_eq!(k.as_str(), "fetched:invoices.all:[]");
    }

    #[test]
    fn fetched_key_with_text_arg_escapes() {
        let k = make_fetched_key("users.by_name", &[Value::Text("a\"b\\c".into())]);
        assert_eq!(k.as_str(), r#"fetched:users.by_name:["a\"b\\c"]"#);
    }

    #[test]
    fn fetched_key_distinguishes_int_args() {
        let a = make_fetched_key("invoices.by_id", &[Value::Int(1)]);
        let b = make_fetched_key("invoices.by_id", &[Value::Int(2)]);
        assert_ne!(a, b);
    }

    #[test]
    fn derived_key_is_deterministic_under_param_reorder() {
        let mut p1 = HashMap::new();
        p1.insert("a".to_string(), ParamValue::Int(1));
        p1.insert("b".to_string(), ParamValue::Int(2));
        let mut p2 = HashMap::new();
        p2.insert("b".to_string(), ParamValue::Int(2));
        p2.insert("a".to_string(), ParamValue::Int(1));
        let k1 = make_derived_key("SELECT 1", &p1, &[]);
        let k2 = make_derived_key("SELECT 1", &p2, &[]);
        assert_eq!(k1, k2);
    }

    #[test]
    fn derived_key_is_deterministic_under_upstream_reorder() {
        let a = RequirementKey::new("fetched:x:[]");
        let b = RequirementKey::new("fetched:y:[]");
        let k1 = make_derived_key("SELECT 1", &HashMap::new(), &[a.clone(), b.clone()]);
        let k2 = make_derived_key("SELECT 1", &HashMap::new(), &[b, a]);
        assert_eq!(k1, k2);
    }

    #[test]
    fn derived_key_changes_with_upstream_set() {
        let a = RequirementKey::new("fetched:invoices.by_id:[1]");
        let b = RequirementKey::new("fetched:invoices.by_id:[2]");
        let k1 = make_derived_key("SELECT * FROM invoices", &HashMap::new(), &[a]);
        let k2 = make_derived_key("SELECT * FROM invoices", &HashMap::new(), &[b]);
        assert_ne!(k1, k2);
    }

    #[test]
    fn derived_key_changes_with_sql() {
        let k1 = make_derived_key("SELECT 1", &HashMap::new(), &[]);
        let k2 = make_derived_key("SELECT 2", &HashMap::new(), &[]);
        assert_ne!(k1, k2);
    }
}
