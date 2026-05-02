//! End-to-end tests of the public API: macros + render pipeline.
//! Only uses items exposed via `sqlbuilder::*` — no internals.

use sqlbuilder::{sql, SqlStmt, Value};

fn binds(stmt: SqlStmt) -> (String, Vec<(String, Value)>) {
    let r = stmt.render().expect("render");
    (r.sql, r.params)
}

fn names(params: &[(String, Value)]) -> Vec<&str> {
    params.iter().map(|(k, _)| k.as_str()).collect()
}

fn as_text(v: &Value) -> &str {
    match v {
        Value::Text(s) => s.as_str(),
        _ => panic!("expected text, got {v:?}"),
    }
}

fn as_int(v: &Value) -> i64 {
    match v {
        Value::Int(n) => *n,
        _ => panic!("expected int, got {v:?}"),
    }
}

// ── sql! capture from scope ───────────────────────────────────────────────

#[test]
fn capture_from_scope_uses_local_binding() {
    let id: i64 = 42;
    let (sql, params) = binds(sql!("SELECT * FROM x WHERE id = {id}"));
    assert_eq!(sql, "SELECT * FROM x WHERE id = :id");
    assert_eq!(names(&params), vec!["id"]);
    assert_eq!(as_int(&params[0].1), 42);
}

#[test]
fn capture_works_for_string_and_str() {
    let name = String::from("Alice");
    let role: &str = "admin";
    let (_, params) = binds(sql!("INSERT INTO u (name, role) VALUES ({name}, {role})"));
    assert_eq!(names(&params), vec!["name", "role"]);
    assert_eq!(as_text(&params[0].1), "Alice");
    assert_eq!(as_text(&params[1].1), "admin");
}

#[test]
fn capture_works_for_borrowed_locals() {
    let owner = String::from("Bob");
    let id: i64 = 7;
    let owner_ref: &String = &owner;
    let id_ref: &i64 = &id;
    let stmt = {
        let owner = owner_ref;
        let id = id_ref;
        sql!("UPDATE u SET name = {owner} WHERE id = {id}")
    };
    let (_, params) = binds(stmt);
    assert_eq!(as_text(&params[0].1), "Bob");
    assert_eq!(as_int(&params[1].1), 7);
}

// ── sql! explicit bindings ────────────────────────────────────────────────

#[test]
fn explicit_binding_overrides_scope() {
    let id: i64 = 1;
    let _ = id;
    let (_, params) = binds(sql!("SELECT * FROM x WHERE id = {id}", id = 99_i64));
    assert_eq!(as_int(&params[0].1), 99);
}

#[test]
fn explicit_binding_with_field_access() {
    struct S {
        id: i64,
    }
    let s = S { id: 13 };
    let (_, params) = binds(sql!("SELECT * FROM x WHERE id = {id}", id = s.id));
    assert_eq!(as_int(&params[0].1), 13);
}

#[test]
fn explicit_binding_renames_sql_placeholder_to_local_name() {
    let local: i64 = 5;
    let (_, params) = binds(sql!(
        "SELECT * FROM x WHERE col = {placeholder}",
        placeholder = local,
    ));
    assert_eq!(names(&params), vec!["placeholder"]);
    assert_eq!(as_int(&params[0].1), 5);
}

// ── sql! placeholder semantics ────────────────────────────────────────────

#[test]
fn same_placeholder_used_twice_creates_one_bind() {
    let id: i64 = 9;
    let (sql, params) = binds(sql!("SELECT * FROM x WHERE a = {id} OR b = {id}"));
    assert_eq!(sql, "SELECT * FROM x WHERE a = :id OR b = :id");
    assert_eq!(params.len(), 1);
}

#[test]
fn brace_placeholder_inside_quoted_string_is_left_alone() {
    let real: i64 = 1;
    let (sql, params) = binds(sql!("SELECT '{not_a_param}', {real}"));
    assert_eq!(sql, "SELECT '{not_a_param}', :real");
    assert_eq!(names(&params), vec!["real"]);
}

#[test]
fn raw_engine_colon_placeholders_pass_through_untouched() {
    // `:name` is engine-level syntax — the macro/renderer leave it alone so
    // backends can still see their own placeholders.
    let real: i64 = 1;
    let (sql, _) = binds(sql!("SELECT :raw_engine_param, {real}"));
    assert_eq!(sql, "SELECT :raw_engine_param, :real");
}

#[test]
fn line_comment_does_not_consume_real_placeholder() {
    let real: i64 = 1;
    let (sql, params) = binds(sql!("-- {commented out}\nSELECT {real}"));
    assert!(sql.contains("-- {commented out}"));
    assert_eq!(names(&params), vec!["real"]);
}

#[test]
fn block_comment_does_not_consume_real_placeholder() {
    let real: i64 = 1;
    let (sql, params) = binds(sql!("/* {commented} */ SELECT {real}"));
    assert!(sql.contains("/* {commented} */"));
    assert_eq!(names(&params), vec!["real"]);
}

#[test]
fn doubled_braces_collapse_to_literal_braces() {
    let (sql, _) = binds(sql!("SELECT {{literal}}"));
    assert_eq!(sql, "SELECT {literal}");
}

// ── Option<T>, bool, Null ─────────────────────────────────────────────────

#[test]
fn option_some_carries_inner_value() {
    let opt: Option<i64> = Some(7);
    let (_, params) = binds(sql!("UPDATE x SET v = {opt}", opt = opt));
    assert_eq!(as_int(&params[0].1), 7);
}

#[test]
fn option_none_renders_as_null() {
    let opt: Option<i64> = None;
    let (_, params) = binds(sql!("UPDATE x SET v = {opt}", opt = opt));
    assert!(matches!(params[0].1, Value::Null));
}

#[test]
fn bool_renders_as_int_zero_one() {
    let yes = true;
    let no = false;
    let (_, params) = binds(sql!("INSERT INTO x (a, b) VALUES ({yes}, {no})"));
    assert_eq!(as_int(&params[0].1), 1);
    assert_eq!(as_int(&params[1].1), 0);
}

// ── Composition: fragments ────────────────────────────────────────────────

#[test]
fn fragment_splice_renames_inner_placeholders() {
    let cond = sql!("status = {status}", status = "active");
    let outer = sql!(
        "SELECT * FROM x WHERE {cond} AND tid = {tid}",
        cond = cond,
        tid = 7_i64,
    );
    let (sql, params) = binds(outer);
    assert_eq!(sql, "SELECT * FROM x WHERE (status = :f0_status) AND tid = :tid");
    assert_eq!(names(&params), vec!["f0_status", "tid"]);
    assert_eq!(as_text(&params[0].1), "active");
    assert_eq!(as_int(&params[1].1), 7);
}

#[test]
fn nested_fragments_resolve_through_two_levels() {
    let inner = sql!("a = {a}", a = 1_i64);
    let middle = sql!("({inner} OR b = {b})", inner = inner, b = 2_i64);
    let outer = sql!("SELECT 1 WHERE {middle}", middle = middle);
    let (sql, params) = binds(outer);
    assert!(sql.starts_with("SELECT 1 WHERE (("));
    assert_eq!(params.len(), 2);
    assert_eq!(as_int(&params[0].1), 1);
    assert_eq!(as_int(&params[1].1), 2);
}

#[test]
fn sibling_fragments_use_independent_prefixes() {
    let a = sql!("col = {v}", v = 1_i64);
    let b = sql!("col = {v}", v = 2_i64);
    let outer = sql!("SELECT 1 WHERE {a} OR {b}", a = a, b = b);
    let (_, params) = binds(outer);
    assert_eq!(params.len(), 2);
    assert_eq!(as_int(&params[0].1), 1);
    assert_eq!(as_int(&params[1].1), 2);
    assert_ne!(params[0].0, params[1].0);
}

// ── Dotted-path capture (`{self.field}`) ─────────────────────────────────

#[test]
fn dotted_path_captures_struct_field() {
    struct S {
        id: i64,
        name: String,
    }
    let s = S { id: 42, name: "Alice".into() };
    let (sql, params) = binds(sql!(
        "UPDATE t SET name = {s.name} WHERE id = {s.id}"
    ));
    // `:self.id` is invalid engine syntax, so dotted names get rewritten.
    assert!(!sql.contains(':') || !sql.contains('.'));
    assert_eq!(params.len(), 2);
    assert_eq!(as_text(&params[0].1), "Alice");
    assert_eq!(as_int(&params[1].1), 42);
}

#[test]
fn dotted_path_works_through_borrowed_self() {
    // Mimics the real `&self` Command::execute_optimistic case.
    struct S {
        id: i64,
    }
    fn run(this: &S) -> Vec<(String, Value)> {
        binds(sql!("DELETE FROM t WHERE id = {this.id}")).1
    }
    let s = S { id: 7 };
    let p = run(&s);
    assert_eq!(as_int(&p[0].1), 7);
}

#[test]
fn dotted_path_walks_nested_structs() {
    struct Inner {
        value: i64,
    }
    struct Outer {
        inner: Inner,
        label: String,
    }
    let o = Outer {
        inner: Inner { value: 99 },
        label: "tag".into(),
    };
    let (_, params) = binds(sql!(
        "INSERT INTO t (v, l) VALUES ({o.inner.value}, {o.label})"
    ));
    assert_eq!(params.len(), 2);
    assert_eq!(as_int(&params[0].1), 99);
    assert_eq!(as_text(&params[1].1), "tag");
}

#[test]
fn dotted_path_used_twice_shares_one_bind() {
    struct S {
        id: i64,
    }
    let s = S { id: 9 };
    let (sql, params) = binds(sql!(
        "SELECT * FROM x WHERE a = {s.id} OR b = {s.id}"
    ));
    assert_eq!(params.len(), 1);
    // Same placeholder name reused — should resolve to same bind.
    let bind_name = &params[0].0;
    assert_eq!(sql.matches(&format!(":{bind_name}")).count(), 2);
}

// ── List parameters ──────────────────────────────────────────────────────

#[test]
fn vec_renders_as_list_param_value() {
    let ids: Vec<i64> = vec![1, 2, 3];
    let (_, params) = binds(sql!("SELECT * FROM x WHERE id IN ({ids})", ids = ids));
    match &params[0].1 {
        Value::IntList(xs) => assert_eq!(xs, &vec![1, 2, 3]),
        other => panic!("expected IntList, got {other:?}"),
    }
}
