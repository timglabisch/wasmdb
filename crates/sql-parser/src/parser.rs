mod core;
mod create_table;
mod delete;
mod insert;
mod lexer;
pub(crate) mod select;
pub mod token;
mod update;

use crate::ast::*;
pub use token::{ParseError, Span, Token, TokenKind};

// ── Public API ──────────────────────────────────────────────────────────

pub fn parse(input: &str) -> Result<AstSelect, ParseError> {
    let mut p = core::ParserCore::new(input);
    select::parse_select(&mut p)
}

pub fn parse_statement(input: &str) -> Result<Statement, ParseError> {
    let mut p = core::ParserCore::new(input);
    let stmt = parse_statement_inner(&mut p)?;
    // optional trailing semicolon
    if p.at(&TokenKind::Semicolon)? {
        p.eat()?;
    }
    p.expect_eof()?;
    Ok(stmt)
}

pub fn parse_statements(input: &str) -> Result<Vec<Statement>, ParseError> {
    let mut p = core::ParserCore::new(input);
    let mut stmts = Vec::new();
    loop {
        if p.at(&TokenKind::Eof)? {
            break;
        }
        stmts.push(parse_statement_inner(&mut p)?);
        if p.at(&TokenKind::Semicolon)? {
            p.eat()?;
        } else {
            p.expect_eof()?;
            break;
        }
    }
    Ok(stmts)
}

fn parse_statement_inner(p: &mut core::ParserCore) -> Result<Statement, ParseError> {
    match p.peek()?.kind {
        TokenKind::Select => Ok(Statement::Select(select::parse_select_inner(p)?)),
        TokenKind::Insert => Ok(Statement::Insert(insert::parse_insert(p)?)),
        TokenKind::Delete => Ok(Statement::Delete(delete::parse_delete(p)?)),
        TokenKind::Update => Ok(Statement::Update(update::parse_update(p)?)),
        TokenKind::Create => Ok(Statement::CreateTable(create_table::parse_create_table(p)?)),
        _ => {
            let tok = p.peek()?.clone();
            Err(ParseError::new(
                format!("expected SELECT, INSERT, DELETE, UPDATE or CREATE, got {}", tok.kind.name()),
                tok.span,
            ))
        }
    }
}

// ── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_select() {
        let ast = parse("SELECT users.name FROM users").unwrap();
        assert_eq!(ast.sources.len(), 1);
        assert!(matches!(&ast.sources[0].source, AstSource::Table(t) if t == "users"));
        assert!(ast.sources[0].join.is_none());
        assert_eq!(ast.result_columns.len(), 1);
        assert!(matches!(
            &ast.result_columns[0].expr,
            AstExpr::Column(AstColumnRef { table, column })
            if table == "users" && column == "name"
        ));
    }

    #[test]
    fn test_parse_multiple_columns() {
        let ast = parse("SELECT users.name, users.age FROM users").unwrap();
        assert_eq!(ast.result_columns.len(), 2);
    }

    #[test]
    fn test_parse_where_clause() {
        let ast = parse("SELECT users.name FROM users WHERE users.age > 18").unwrap();
        assert_eq!(ast.filter.len(), 1);
        assert!(matches!(
            &ast.filter[0],
            AstExpr::Binary { op: Operator::Gt, .. }
        ));
    }

    #[test]
    fn test_parse_where_and() {
        let ast =
            parse("SELECT users.name FROM users WHERE users.age > 18 AND users.name = 'Alice'")
                .unwrap();
        assert_eq!(ast.filter.len(), 1);
        assert!(matches!(
            &ast.filter[0],
            AstExpr::Binary { op: Operator::And, .. }
        ));
    }

    #[test]
    fn test_parse_literal_on_left() {
        let ast = parse("SELECT users.name FROM users WHERE 18 < users.age").unwrap();
        assert_eq!(ast.filter.len(), 1);
        assert!(matches!(
            &ast.filter[0],
            AstExpr::Binary { op: Operator::Lt, .. }
        ));
    }

    #[test]
    fn test_parse_inner_join() {
        let ast = parse(
            "SELECT users.name FROM users INNER JOIN orders ON users.id = orders.user_id",
        )
        .unwrap();
        assert_eq!(ast.sources.len(), 2);
        assert!(matches!(&ast.sources[1].source, AstSource::Table(t) if t == "orders"));
        let join = ast.sources[1].join.as_ref().unwrap();
        assert_eq!(join.join_type, JoinType::Inner);
        assert_eq!(join.on.len(), 1);
    }

    #[test]
    fn test_parse_left_join() {
        let ast =
            parse("SELECT users.name FROM users LEFT JOIN orders ON users.id = orders.user_id")
                .unwrap();
        let join = ast.sources[1].join.as_ref().unwrap();
        assert_eq!(join.join_type, JoinType::Left);
    }

    #[test]
    fn test_parse_bare_join() {
        let ast =
            parse("SELECT users.name FROM users JOIN orders ON users.id = orders.user_id")
                .unwrap();
        let join = ast.sources[1].join.as_ref().unwrap();
        assert_eq!(join.join_type, JoinType::Inner);
    }

    #[test]
    fn test_parse_three_table_join() {
        let ast = parse(
            "SELECT users.name FROM users \
             INNER JOIN orders ON users.id = orders.user_id \
             LEFT JOIN products ON orders.id = products.id",
        )
        .unwrap();
        assert_eq!(ast.sources.len(), 3);
        assert!(matches!(&ast.sources[0].source, AstSource::Table(t) if t == "users"));
        assert!(matches!(&ast.sources[1].source, AstSource::Table(t) if t == "orders"));
        assert!(matches!(&ast.sources[2].source, AstSource::Table(t) if t == "products"));
        assert_eq!(
            ast.sources[1].join.as_ref().unwrap().join_type,
            JoinType::Inner
        );
        assert_eq!(
            ast.sources[2].join.as_ref().unwrap().join_type,
            JoinType::Left
        );
    }

    #[test]
    fn test_parse_group_by() {
        let ast = parse("SELECT users.name FROM users GROUP BY users.name").unwrap();
        assert_eq!(ast.group_by.len(), 1);
        assert!(matches!(
            &ast.group_by[0],
            AstExpr::Column(AstColumnRef { table, column })
            if table == "users" && column == "name"
        ));
    }

    #[test]
    fn test_parse_aggregate() {
        let ast = parse("SELECT MIN(users.age) FROM users").unwrap();
        assert!(matches!(
            &ast.result_columns[0].expr,
            AstExpr::Aggregate { func: AggFunc::Min, .. }
        ));
    }

    #[test]
    fn test_parse_aggregate_with_alias() {
        let ast = parse("SELECT MIN(users.age) AS min_age FROM users").unwrap();
        assert_eq!(ast.result_columns[0].alias, Some("min_age".to_string()));
    }

    #[test]
    fn test_parse_all_aggregates() {
        let ast =
            parse("SELECT COUNT(u.x), SUM(u.x), MIN(u.x), MAX(u.x) FROM u").unwrap();
        assert!(matches!(
            &ast.result_columns[0].expr,
            AstExpr::Aggregate { func: AggFunc::Count, .. }
        ));
        assert!(matches!(
            &ast.result_columns[1].expr,
            AstExpr::Aggregate { func: AggFunc::Sum, .. }
        ));
        assert!(matches!(
            &ast.result_columns[2].expr,
            AstExpr::Aggregate { func: AggFunc::Min, .. }
        ));
        assert!(matches!(
            &ast.result_columns[3].expr,
            AstExpr::Aggregate { func: AggFunc::Max, .. }
        ));
    }

    #[test]
    fn test_parse_complex_query() {
        let ast = parse(
            "SELECT users.name, MIN(users.age) AS min_age \
             FROM users \
             INNER JOIN orders ON users.id = orders.user_id \
             WHERE users.age > 18 AND users.name = 'Alice' \
             GROUP BY users.name",
        )
        .unwrap();
        assert_eq!(ast.result_columns.len(), 2);
        assert_eq!(ast.sources.len(), 2);
        assert_eq!(ast.filter.len(), 1);
        assert_eq!(ast.group_by.len(), 1);
    }

    #[test]
    fn test_parse_or_precedence() {
        let ast =
            parse("SELECT u.x FROM u WHERE u.a = 1 OR u.b = 2 AND u.c = 3").unwrap();
        assert!(matches!(
            &ast.filter[0],
            AstExpr::Binary { op: Operator::Or, .. }
        ));
        if let AstExpr::Binary { right, .. } = &ast.filter[0] {
            assert!(matches!(
                right.as_ref(),
                AstExpr::Binary { op: Operator::And, .. }
            ));
        }
    }

    #[test]
    fn test_parse_parenthesized_expr() {
        let ast =
            parse("SELECT u.x FROM u WHERE (u.a = 1 OR u.b = 2) AND u.c = 3").unwrap();
        assert!(matches!(
            &ast.filter[0],
            AstExpr::Binary { op: Operator::And, .. }
        ));
    }

    // ── Error tests ─────────────────────────────────────────────────────

    #[test]
    fn test_error_missing_from() {
        let err = parse("SELECT users.name WHERE").unwrap_err();
        assert!(err.message.contains("expected FROM"));
    }

    #[test]
    fn test_error_missing_table_after_join() {
        let err = parse("SELECT u.x FROM users JOIN ON u.id = u.id").unwrap_err();
        assert!(err.message.contains("expected identifier"));
    }

    #[test]
    fn test_error_missing_on() {
        let err =
            parse("SELECT u.x FROM users JOIN orders WHERE u.id = u.id").unwrap_err();
        assert!(err.message.contains("expected ON"));
    }

    #[test]
    fn test_error_render() {
        let input = "SELECT users.name FORM users";
        let err = parse(input).unwrap_err();
        let rendered = err.render(input);
        assert!(rendered.contains("parse error"));
        assert!(rendered.contains("^"));
        assert!(rendered.contains("FORM"));
    }

    #[test]
    fn test_error_unexpected_token_in_expr() {
        let err = parse("SELECT FROM users").unwrap_err();
        assert!(err.message.contains("expected expression"));
    }

    // ── IN + Subquery tests ─────────────────────────────────────────────

    #[test]
    fn test_parse_in_list() {
        let ast = parse("SELECT u.x FROM u WHERE u.id IN (1, 2, 3)").unwrap();
        assert!(matches!(&ast.filter[0], AstExpr::InList { values, .. } if values.len() == 3));
    }

    #[test]
    fn test_parse_in_strings() {
        let ast = parse("SELECT u.x FROM u WHERE u.name IN ('Alice', 'Bob')").unwrap();
        if let AstExpr::InList { values, .. } = &ast.filter[0] {
            assert_eq!(values.len(), 2);
            assert!(matches!(&values[0], AstExpr::Literal(Value::Text(s)) if s == "Alice"));
            assert!(matches!(&values[1], AstExpr::Literal(Value::Text(s)) if s == "Bob"));
        } else {
            panic!("expected InList");
        }
    }

    #[test]
    fn test_parse_in_subquery() {
        let ast = parse(
            "SELECT u.x FROM u WHERE u.id IN (SELECT o.user_id FROM o WHERE o.amount > 100)"
        ).unwrap();
        assert!(matches!(&ast.filter[0], AstExpr::InSubquery { .. }));
    }

    #[test]
    fn test_parse_scalar_subquery() {
        let ast = parse(
            "SELECT u.x FROM u WHERE u.age > (SELECT MIN(o.amount) FROM o)"
        ).unwrap();
        assert!(matches!(
            &ast.filter[0],
            AstExpr::Binary { op: Operator::Gt, right, .. }
            if matches!(right.as_ref(), AstExpr::Subquery(_))
        ));
    }

    #[test]
    fn test_parse_in_with_and() {
        let ast = parse(
            "SELECT u.x FROM u WHERE u.id IN (1, 2) AND u.age > 18"
        ).unwrap();
        assert!(matches!(&ast.filter[0], AstExpr::Binary { op: Operator::And, .. }));
    }

    // ── Placeholder tests ──────────────────────────────────────────────

    #[test]
    fn test_parse_placeholder_in_where() {
        let ast = parse("SELECT u.x FROM u WHERE u.id = :user_id").unwrap();
        assert!(matches!(
            &ast.filter[0],
            AstExpr::Binary {
                op: Operator::Eq,
                right,
                ..
            } if matches!(right.as_ref(), AstExpr::Literal(Value::Placeholder(name)) if name == "user_id")
        ));
    }

    #[test]
    fn test_parse_placeholder_in_list() {
        let ast = parse("SELECT u.x FROM u WHERE u.id IN (:ids)").unwrap();
        match &ast.filter[0] {
            AstExpr::InList { values, .. } => {
                assert_eq!(values.len(), 1);
                assert!(matches!(&values[0], AstExpr::Literal(Value::Placeholder(name)) if name == "ids"));
            }
            _ => panic!("expected InList"),
        }
    }

    #[test]
    fn test_parse_placeholder_in_limit() {
        let ast = parse("SELECT u.x FROM u LIMIT :n").unwrap();
        assert!(matches!(ast.limit, Some(AstLimit::Placeholder(ref name)) if name == "n"));
    }

    #[test]
    fn test_parse_multiple_placeholders() {
        let ast = parse("SELECT u.x FROM u WHERE u.id = :id AND u.name = :name").unwrap();
        assert!(matches!(&ast.filter[0], AstExpr::Binary { op: Operator::And, .. }));
    }

    #[test]
    fn test_parse_same_placeholder_twice() {
        let ast = parse("SELECT u.x FROM u WHERE u.a = :val OR u.b = :val").unwrap();
        assert!(matches!(&ast.filter[0], AstExpr::Binary { op: Operator::Or, .. }));
    }

    #[test]
    fn test_parse_limit_integer_still_works() {
        let ast = parse("SELECT u.x FROM u LIMIT 10").unwrap();
        assert!(matches!(ast.limit, Some(AstLimit::Value(10))));
    }

    // ── REACTIVE tests ──────────────────────────────────────────────────

    #[test]
    fn test_parse_reactive_simple() {
        let ast = parse("SELECT REACTIVE(users.id = :uid) AS inv, users.name FROM users").unwrap();
        assert_eq!(ast.result_columns.len(), 2);
        assert!(matches!(&ast.result_columns[0].expr, AstExpr::Reactive(_)));
        assert_eq!(ast.result_columns[0].alias, Some("inv".to_string()));
        if let AstExpr::Reactive(inner) = &ast.result_columns[0].expr {
            assert!(matches!(inner.as_ref(), AstExpr::Binary { op: Operator::Eq, .. }));
        }
    }

    #[test]
    fn test_parse_reactive_compound() {
        let ast = parse(
            "SELECT REACTIVE(users.id = :uid AND users.age > 18) FROM users"
        ).unwrap();
        if let AstExpr::Reactive(inner) = &ast.result_columns[0].expr {
            assert!(matches!(inner.as_ref(), AstExpr::Binary { op: Operator::And, .. }));
        } else {
            panic!("expected Reactive");
        }
    }

    #[test]
    fn test_parse_reactive_case_insensitive() {
        let ast = parse("SELECT reactive(users.id = :uid) FROM users").unwrap();
        assert!(matches!(&ast.result_columns[0].expr, AstExpr::Reactive(_)));
    }

    // ── INSERT tests ────────────────────────────────────────────────────

    #[test]
    fn test_parse_insert_simple() {
        let stmt = parse_statement("INSERT INTO users VALUES (1, 'Alice', 30)").unwrap();
        match stmt {
            Statement::Insert(ins) => {
                assert_eq!(ins.table, "users");
                assert!(ins.columns.is_empty());
                assert_eq!(ins.values.len(), 1);
                assert_eq!(ins.values[0].len(), 3);
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn test_parse_insert_with_columns() {
        let stmt = parse_statement("INSERT INTO users (id, name) VALUES (1, 'Alice')").unwrap();
        match stmt {
            Statement::Insert(ins) => {
                assert_eq!(ins.columns, vec!["id", "name"]);
                assert_eq!(ins.values.len(), 1);
                assert_eq!(ins.values[0].len(), 2);
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn test_parse_insert_multi_row() {
        let stmt = parse_statement("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").unwrap();
        match stmt {
            Statement::Insert(ins) => {
                assert_eq!(ins.values.len(), 2);
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn test_parse_insert_with_null() {
        let stmt = parse_statement("INSERT INTO users VALUES (1, NULL)").unwrap();
        match stmt {
            Statement::Insert(ins) => {
                assert!(matches!(&ins.values[0][1], AstExpr::Literal(Value::Null)));
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn test_parse_insert_with_placeholder() {
        let stmt = parse_statement("INSERT INTO users VALUES (:id, :name)").unwrap();
        match stmt {
            Statement::Insert(ins) => {
                assert!(matches!(&ins.values[0][0], AstExpr::Literal(Value::Placeholder(n)) if n == "id"));
                assert!(matches!(&ins.values[0][1], AstExpr::Literal(Value::Placeholder(n)) if n == "name"));
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn test_parse_statement_select() {
        let stmt = parse_statement("SELECT users.name FROM users").unwrap();
        assert!(matches!(stmt, Statement::Select(_)));
    }

    #[test]
    fn test_parse_placeholder_on_left_side() {
        let ast = parse("SELECT u.x FROM u WHERE :val < u.age").unwrap();
        assert!(matches!(
            &ast.filter[0],
            AstExpr::Binary { op: Operator::Lt, left, .. }
            if matches!(left.as_ref(), AstExpr::Literal(Value::Placeholder(name)) if name == "val")
        ));
    }

    // ── CREATE TABLE tests ─────────────────────────────────────────────

    #[test]
    fn test_parse_create_table_simple() {
        let stmt = parse_statement("CREATE TABLE users (id I64, name STRING)").unwrap();
        match stmt {
            Statement::CreateTable(ct) => {
                assert_eq!(ct.name, "users");
                assert_eq!(ct.columns.len(), 2);
                assert_eq!(ct.columns[0].name, "id");
                assert_eq!(ct.columns[0].data_type, AstDataType::I64);
                assert!(!ct.columns[0].not_null);
                assert_eq!(ct.columns[1].name, "name");
                assert_eq!(ct.columns[1].data_type, AstDataType::String);
            }
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_create_table_uuid_column() {
        let stmt = parse_statement(
            "CREATE TABLE customers (id UUID NOT NULL PRIMARY KEY, name STRING)"
        ).unwrap();
        match stmt {
            Statement::CreateTable(ct) => {
                assert_eq!(ct.columns[0].data_type, AstDataType::Uuid);
                assert!(ct.columns[0].not_null);
                assert!(ct.columns[0].primary_key);
            }
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_uuid_literal_in_where() {
        let select = parse(
            "SELECT customers.name FROM customers \
             WHERE customers.id = UUID '550e8400-e29b-41d4-a716-446655440000'"
        ).unwrap();
        let filter = &select.filter[0];
        match filter {
            AstExpr::Binary { right, .. } => match right.as_ref() {
                AstExpr::Literal(Value::Uuid(b)) => {
                    assert_eq!(
                        crate::uuid::format_uuid(b),
                        "550e8400-e29b-41d4-a716-446655440000",
                    );
                }
                other => panic!("expected Uuid literal, got {other:?}"),
            },
            other => panic!("expected Binary, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_uuid_literal_invalid() {
        let err = parse(
            "SELECT customers.name FROM customers \
             WHERE customers.id = UUID 'not-a-uuid'"
        ).unwrap_err();
        assert!(err.message.contains("invalid UUID"), "got: {}", err.message);
    }

    #[test]
    fn test_parse_create_table_uuid_nullable() {
        let stmt = parse_statement(
            "CREATE TABLE customers (id UUID NOT NULL PRIMARY KEY, external UUID)"
        ).unwrap();
        match stmt {
            Statement::CreateTable(ct) => {
                assert_eq!(ct.columns[0].data_type, AstDataType::Uuid);
                assert!(ct.columns[0].not_null);
                assert_eq!(ct.columns[1].data_type, AstDataType::Uuid);
                assert!(!ct.columns[1].not_null);
            }
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_create_table_uuid_lowercase_keyword() {
        let stmt = parse_statement(
            "CREATE TABLE t (id uuid NOT NULL PRIMARY KEY)"
        ).unwrap();
        match stmt {
            Statement::CreateTable(ct) => {
                assert_eq!(ct.columns[0].data_type, AstDataType::Uuid);
            }
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_create_table_uuid_mixed_case_keyword() {
        let stmt = parse_statement(
            "CREATE TABLE t (id Uuid NOT NULL PRIMARY KEY)"
        ).unwrap();
        match stmt {
            Statement::CreateTable(ct) => {
                assert_eq!(ct.columns[0].data_type, AstDataType::Uuid);
            }
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_create_table_composite_pk_with_uuid() {
        let stmt = parse_statement(
            "CREATE TABLE customers (\
                tenant_id I64 NOT NULL, \
                id UUID NOT NULL, \
                name STRING, \
                PRIMARY KEY (tenant_id, id)\
            )"
        ).unwrap();
        match stmt {
            Statement::CreateTable(ct) => {
                assert_eq!(ct.columns[0].data_type, AstDataType::I64);
                assert_eq!(ct.columns[1].data_type, AstDataType::Uuid);
                let has_pk = ct.constraints.iter().any(|c| matches!(c,
                    AstTableConstraint::PrimaryKey { columns } if columns == &["tenant_id", "id"]
                ));
                assert!(has_pk, "expected composite PK constraint");
            }
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_create_table_uuid_with_hash_index() {
        let stmt = parse_statement(
            "CREATE TABLE t (id UUID NOT NULL PRIMARY KEY, INDEX idx_id (id) USING HASH)"
        ).unwrap();
        match stmt {
            Statement::CreateTable(ct) => {
                let has_hash = ct.constraints.iter().any(|c| matches!(c,
                    AstTableConstraint::Index { index_type, .. } if *index_type == AstIndexType::Hash
                ));
                assert!(has_hash, "expected hash index");
            }
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_create_table_uuid_with_btree_index() {
        let stmt = parse_statement(
            "CREATE TABLE t (id UUID NOT NULL PRIMARY KEY, INDEX idx_id (id) USING BTREE)"
        ).unwrap();
        match stmt {
            Statement::CreateTable(ct) => {
                let has_btree = ct.constraints.iter().any(|c| matches!(c,
                    AstTableConstraint::Index { index_type, .. } if *index_type == AstIndexType::BTree
                ));
                assert!(has_btree, "expected btree index");
            }
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_uuid_literal_in_in_list() {
        let select = parse(
            "SELECT customers.name FROM customers \
             WHERE customers.id IN (\
                UUID '550e8400-e29b-41d4-a716-446655440000', \
                UUID '00000000-0000-0000-0000-000000000001'\
             )"
        ).unwrap();
        match &select.filter[0] {
            AstExpr::InList { values, .. } => {
                assert_eq!(values.len(), 2);
                assert!(matches!(&values[0], AstExpr::Literal(Value::Uuid(_))));
                assert!(matches!(&values[1], AstExpr::Literal(Value::Uuid(_))));
            }
            other => panic!("expected InList, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_uuid_literal_in_insert() {
        let stmt = parse_statement(
            "INSERT INTO customers (id, name) \
             VALUES (UUID '550e8400-e29b-41d4-a716-446655440000', 'Alice')"
        ).unwrap();
        match stmt {
            Statement::Insert(ins) => {
                assert_eq!(ins.values[0].len(), 2);
                assert!(matches!(&ins.values[0][0], AstExpr::Literal(Value::Uuid(_))));
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn test_parse_uuid_literal_in_update() {
        let stmt = parse_statement(
            "UPDATE customers \
             SET id = UUID '550e8400-e29b-41d4-a716-446655440000' \
             WHERE customers.name = 'Alice'"
        ).unwrap();
        match stmt {
            Statement::Update(upd) => {
                assert_eq!(upd.assignments.len(), 1);
                assert!(matches!(&upd.assignments[0].1, AstExpr::Literal(Value::Uuid(_))));
            }
            _ => panic!("expected Update"),
        }
    }

    #[test]
    fn test_parse_uuid_literal_in_delete() {
        let stmt = parse_statement(
            "DELETE FROM customers \
             WHERE customers.id = UUID '550e8400-e29b-41d4-a716-446655440000'"
        ).unwrap();
        match stmt {
            Statement::Delete(del) => {
                let filter = del.filter.expect("filter");
                assert!(matches!(
                    &filter,
                    AstExpr::Binary { right, .. }
                        if matches!(right.as_ref(), AstExpr::Literal(Value::Uuid(_)))
                ));
            }
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn test_parse_uuid_literal_in_join_on() {
        let select = parse(
            "SELECT users.id FROM users \
             INNER JOIN customers ON customers.id = UUID '550e8400-e29b-41d4-a716-446655440000'"
        ).unwrap();
        let join = select.sources[1].join.as_ref().unwrap();
        let on = &join.on[0];
        assert!(matches!(
            on,
            AstExpr::Binary { right, .. }
                if matches!(right.as_ref(), AstExpr::Literal(Value::Uuid(_)))
        ));
    }

    #[test]
    fn test_parse_uuid_keyword_without_string_errors() {
        let err = parse_statement(
            "SELECT customers.name FROM customers WHERE customers.id = UUID 42"
        ).unwrap_err();
        assert!(
            err.message.contains("UUID string literal"),
            "got: {}",
            err.message
        );
    }

    #[test]
    fn test_parse_create_table_not_null() {
        let stmt = parse_statement("CREATE TABLE t (name STRING NOT NULL)").unwrap();
        match stmt {
            Statement::CreateTable(ct) => assert!(ct.columns[0].not_null),
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_create_table_primary_key() {
        let stmt = parse_statement("CREATE TABLE t (id I64 NOT NULL PRIMARY KEY, name STRING)").unwrap();
        match stmt {
            Statement::CreateTable(ct) => {
                assert!(ct.columns[0].primary_key);
                assert!(ct.columns[0].not_null);
                assert!(!ct.columns[1].primary_key);
            }
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_create_table_pk_implies_not_null() {
        let stmt = parse_statement("CREATE TABLE t (id I64 PRIMARY KEY)").unwrap();
        match stmt {
            Statement::CreateTable(ct) => {
                assert!(ct.columns[0].primary_key);
                assert!(ct.columns[0].not_null);
            }
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_create_table_constraint_pk() {
        let stmt = parse_statement("CREATE TABLE t (a I64, b I64, PRIMARY KEY (a, b))").unwrap();
        match stmt {
            Statement::CreateTable(ct) => {
                assert_eq!(ct.constraints.len(), 1);
                match &ct.constraints[0] {
                    AstTableConstraint::PrimaryKey { columns } => {
                        assert_eq!(columns, &["a", "b"]);
                    }
                    _ => panic!("expected PrimaryKey constraint"),
                }
            }
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_create_table_index() {
        let stmt = parse_statement("CREATE TABLE t (id I64, name STRING, INDEX idx_name (name))").unwrap();
        match stmt {
            Statement::CreateTable(ct) => {
                assert_eq!(ct.constraints.len(), 1);
                match &ct.constraints[0] {
                    AstTableConstraint::Index { name, columns, index_type } => {
                        assert_eq!(name.as_deref(), Some("idx_name"));
                        assert_eq!(columns, &["name"]);
                        assert_eq!(*index_type, AstIndexType::BTree);
                    }
                    _ => panic!("expected Index constraint"),
                }
            }
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_create_table_index_using_hash() {
        let stmt = parse_statement("CREATE TABLE t (id I64, INDEX idx_id (id) USING HASH)").unwrap();
        match stmt {
            Statement::CreateTable(ct) => {
                match &ct.constraints[0] {
                    AstTableConstraint::Index { index_type, .. } => {
                        assert_eq!(*index_type, AstIndexType::Hash);
                    }
                    _ => panic!("expected Index constraint"),
                }
            }
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_create_table_trailing_semicolon() {
        let stmt = parse_statement("CREATE TABLE t (id I64);").unwrap();
        match stmt {
            Statement::CreateTable(ct) => assert_eq!(ct.name, "t"),
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_create_table_case_insensitive() {
        let stmt = parse_statement("create table Users (Id i64 not null primary key)").unwrap();
        match stmt {
            Statement::CreateTable(ct) => {
                assert_eq!(ct.name, "Users");
                assert_eq!(ct.columns[0].name, "Id");
                assert!(ct.columns[0].primary_key);
            }
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_create_table_full() {
        let stmt = parse_statement("
            CREATE TABLE users (
                id I64 NOT NULL PRIMARY KEY,
                name STRING NOT NULL,
                age I64,
                email STRING,
                INDEX idx_email (email),
                INDEX idx_name_age (name, age)
            )
        ").unwrap();
        match stmt {
            Statement::CreateTable(ct) => {
                assert_eq!(ct.name, "users");
                assert_eq!(ct.columns.len(), 4);
                assert_eq!(ct.constraints.len(), 2);
                assert!(ct.columns[0].primary_key);
                assert!(ct.columns[0].not_null);
                assert!(ct.columns[1].not_null);
                assert!(!ct.columns[2].not_null);
                assert!(!ct.columns[3].not_null);
            }
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_create_table_error_missing_type() {
        let err = parse_statement("CREATE TABLE t (id)").unwrap_err();
        assert!(err.message.contains("expected data type"));
    }

    #[test]
    fn test_parse_create_table_error_missing_name() {
        let err = parse_statement("CREATE TABLE (id I64)").unwrap_err();
        assert!(err.message.contains("expected identifier"));
    }

    #[test]
    fn test_parse_create_table_index_without_name() {
        let stmt = parse_statement("CREATE TABLE t (id I64, INDEX (id))").unwrap();
        match stmt {
            Statement::CreateTable(ct) => {
                match &ct.constraints[0] {
                    AstTableConstraint::Index { name, columns, index_type } => {
                        assert!(name.is_none());
                        assert_eq!(columns, &["id"]);
                        assert_eq!(*index_type, AstIndexType::BTree);
                    }
                    _ => panic!("expected Index constraint"),
                }
            }
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_create_table_index_using_btree() {
        let stmt = parse_statement("CREATE TABLE t (id I64, INDEX idx_id (id) USING BTREE)").unwrap();
        match stmt {
            Statement::CreateTable(ct) => {
                match &ct.constraints[0] {
                    AstTableConstraint::Index { index_type, .. } => {
                        assert_eq!(*index_type, AstIndexType::BTree);
                    }
                    _ => panic!("expected Index constraint"),
                }
            }
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_create_table_composite_index() {
        let stmt = parse_statement("CREATE TABLE t (a I64, b STRING, INDEX idx_ab (a, b))").unwrap();
        match stmt {
            Statement::CreateTable(ct) => {
                match &ct.constraints[0] {
                    AstTableConstraint::Index { columns, .. } => {
                        assert_eq!(columns, &["a", "b"]);
                    }
                    _ => panic!("expected Index constraint"),
                }
            }
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_create_table_all_data_types() {
        let stmt = parse_statement("CREATE TABLE t (a STRING, b I64)").unwrap();
        match stmt {
            Statement::CreateTable(ct) => {
                assert_eq!(ct.columns[0].data_type, AstDataType::String);
                assert_eq!(ct.columns[1].data_type, AstDataType::I64);
            }
            _ => panic!("expected CreateTable"),
        }
    }

    // ── DDL keywords as identifiers ────────────────────────────────────

    #[test]
    fn test_ddl_keywords_usable_as_identifiers() {
        // "key", "index", "table", "primary", "hash" etc. must work as column/table names
        let ast = parse("SELECT t.key FROM t WHERE t.index = 1").unwrap();
        assert!(matches!(
            &ast.result_columns[0].expr,
            AstExpr::Column(AstColumnRef { table, column })
            if table == "t" && column == "key"
        ));
        assert!(matches!(
            &ast.filter[0],
            AstExpr::Binary { left, .. }
            if matches!(left.as_ref(), AstExpr::Column(AstColumnRef { column, .. }) if column == "index")
        ));
    }

    #[test]
    fn test_create_table_with_ddl_keyword_column_names() {
        let stmt = parse_statement(
            "CREATE TABLE t (hash I64, key STRING NOT NULL, idx I64)"
        ).unwrap();
        match stmt {
            Statement::CreateTable(ct) => {
                assert_eq!(ct.columns[0].name, "hash");
                assert_eq!(ct.columns[1].name, "key");
                assert_eq!(ct.columns[2].name, "idx");
            }
            _ => panic!("expected CreateTable"),
        }
    }

    // ── parse_statements (multi-statement) tests ──────────────────────

    #[test]
    fn test_parse_statements_single() {
        let stmts = parse_statements("SELECT u.x FROM u").unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(&stmts[0], Statement::Select(_)));
    }

    #[test]
    fn test_parse_statements_single_trailing_semicolon() {
        let stmts = parse_statements("SELECT u.x FROM u;").unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn test_parse_statements_multiple_creates() {
        let stmts = parse_statements(
            "CREATE TABLE users (id I64, name STRING); CREATE TABLE orders (id I64, user_id I64)"
        ).unwrap();
        assert_eq!(stmts.len(), 2);
        assert!(matches!(&stmts[0], Statement::CreateTable(ct) if ct.name == "users"));
        assert!(matches!(&stmts[1], Statement::CreateTable(ct) if ct.name == "orders"));
    }

    #[test]
    fn test_parse_statements_mixed() {
        let stmts = parse_statements(
            "CREATE TABLE t (id I64); INSERT INTO t VALUES (1); SELECT t.id FROM t"
        ).unwrap();
        assert_eq!(stmts.len(), 3);
        assert!(matches!(&stmts[0], Statement::CreateTable(_)));
        assert!(matches!(&stmts[1], Statement::Insert(_)));
        assert!(matches!(&stmts[2], Statement::Select(_)));
    }

    #[test]
    fn test_parse_statements_trailing_semicolons() {
        let stmts = parse_statements(
            "CREATE TABLE t (id I64); INSERT INTO t VALUES (1);"
        ).unwrap();
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn test_parse_statements_empty_input() {
        let stmts = parse_statements("").unwrap();
        assert!(stmts.is_empty());
    }

    #[test]
    fn test_parse_statements_whitespace_only() {
        let stmts = parse_statements("   ").unwrap();
        assert!(stmts.is_empty());
    }

    // ── Function-call sources in FROM ───────────────────────────────────

    fn expect_call(e: &AstSourceEntry) -> (&str, &str, &[AstExpr]) {
        match &e.source {
            AstSource::Call { schema, function, args } => (schema, function, args),
            AstSource::Table(t) => panic!("expected Call, got Table({t})"),
        }
    }

    #[test]
    fn test_parse_call_basic() {
        let ast = parse("SELECT c.name FROM customers.by_owner(42)").unwrap();
        assert_eq!(ast.sources.len(), 1);
        let s = &ast.sources[0];
        let (schema, function, args) = expect_call(s);
        assert_eq!(schema, "customers");
        assert_eq!(function, "by_owner");
        assert!(s.alias.is_none());
        assert_eq!(args.len(), 1);
        assert!(matches!(args[0], AstExpr::Literal(Value::Int(42))));
    }

    #[test]
    fn test_parse_call_with_alias_and_filter() {
        let ast = parse(
            "SELECT c.name FROM customers.by_owner(42) AS c WHERE c.name = 'Alice'",
        ).unwrap();
        let s = &ast.sources[0];
        let (schema, function, _args) = expect_call(s);
        assert_eq!(schema, "customers");
        assert_eq!(function, "by_owner");
        assert_eq!(s.alias.as_deref(), Some("c"));
        assert_eq!(ast.filter.len(), 1);
    }

    #[test]
    fn test_parse_call_join() {
        let ast = parse(
            "SELECT a.x FROM a.f(1) AS a \
             INNER JOIN b.g(2, 'x') AS b ON a.id = b.a_id",
        ).unwrap();
        assert_eq!(ast.sources.len(), 2);

        let left = &ast.sources[0];
        let (ls, lf, la) = expect_call(left);
        assert_eq!(ls, "a");
        assert_eq!(lf, "f");
        assert_eq!(left.alias.as_deref(), Some("a"));
        assert_eq!(la.len(), 1);

        let right = &ast.sources[1];
        let (rs, rf, ra) = expect_call(right);
        assert_eq!(rs, "b");
        assert_eq!(rf, "g");
        assert_eq!(right.alias.as_deref(), Some("b"));
        assert_eq!(ra.len(), 2);
        assert!(matches!(ra[0], AstExpr::Literal(Value::Int(2))));
        assert!(matches!(&ra[1], AstExpr::Literal(Value::Text(s)) if s == "x"));
        assert!(right.join.is_some());
    }

    #[test]
    fn test_parse_plain_table_still_works() {
        let ast = parse("SELECT users.name FROM users").unwrap();
        let s = &ast.sources[0];
        assert!(matches!(&s.source, AstSource::Table(t) if t == "users"));
        assert!(s.alias.is_none());
    }

    #[test]
    fn test_parse_call_no_args() {
        let ast = parse("SELECT c.id FROM customers.list() AS c").unwrap();
        let s = &ast.sources[0];
        let (schema, function, args) = expect_call(s);
        assert_eq!(schema, "customers");
        assert_eq!(function, "list");
        assert_eq!(args.len(), 0);
    }

    #[test]
    fn test_parse_call_three_args() {
        let ast = parse("SELECT c.id FROM customers.by_bucket(1, 2, 3)").unwrap();
        let (_, _, args) = expect_call(&ast.sources[0]);
        assert_eq!(args.len(), 3);
        assert!(matches!(args[0], AstExpr::Literal(Value::Int(1))));
        assert!(matches!(args[1], AstExpr::Literal(Value::Int(2))));
        assert!(matches!(args[2], AstExpr::Literal(Value::Int(3))));
    }

    #[test]
    fn test_parse_call_placeholder_arg() {
        let ast = parse("SELECT c.id FROM customers.by_owner(:owner_id)").unwrap();
        let (_, _, args) = expect_call(&ast.sources[0]);
        assert_eq!(args.len(), 1);
        assert!(matches!(&args[0], AstExpr::Literal(Value::Placeholder(n)) if n == "owner_id"));
    }

    #[test]
    fn test_parse_call_mixed_arg_types() {
        let ast = parse("SELECT c.id FROM customers.by_name(1, 'Alice', NULL)").unwrap();
        let (_, _, args) = expect_call(&ast.sources[0]);
        assert_eq!(args.len(), 3);
        assert!(matches!(args[0], AstExpr::Literal(Value::Int(1))));
        assert!(matches!(&args[1], AstExpr::Literal(Value::Text(s)) if s == "Alice"));
        assert!(matches!(args[2], AstExpr::Literal(Value::Null)));
    }

    #[test]
    fn test_parse_plain_table_with_alias() {
        let ast = parse("SELECT c.name FROM customers AS c").unwrap();
        let s = &ast.sources[0];
        assert!(matches!(&s.source, AstSource::Table(t) if t == "customers"));
        assert_eq!(s.alias.as_deref(), Some("c"));
    }

    #[test]
    fn test_parse_join_plain_table_with_alias() {
        let ast = parse(
            "SELECT a.x FROM users AS a INNER JOIN orders AS b ON a.id = b.user_id",
        ).unwrap();
        assert_eq!(ast.sources[0].alias.as_deref(), Some("a"));
        assert_eq!(ast.sources[1].alias.as_deref(), Some("b"));
    }

    // ── Call error cases ───────────────────────────────────────────────

    #[test]
    fn test_parse_call_error_unclosed_paren() {
        let err = parse("SELECT c.id FROM customers.by_owner(42").unwrap_err();
        assert!(err.message.contains("expected"), "got: {}", err.message);
    }

    #[test]
    fn test_parse_call_error_missing_fn_name() {
        let err = parse("SELECT c.id FROM customers.(42)").unwrap_err();
        assert!(err.message.contains("expected identifier"), "got: {}", err.message);
    }

    #[test]
    fn test_parse_call_error_trailing_comma() {
        let err = parse("SELECT c.id FROM customers.by_owner(1,)").unwrap_err();
        assert!(err.message.contains("expected expression"), "got: {}", err.message);
    }

    #[test]
    fn test_parse_call_error_leading_dot() {
        // bare `.by_owner(42)` should not be accepted — there is no schema.
        let err = parse("SELECT c.id FROM .by_owner(42)").unwrap_err();
        assert!(err.message.contains("expected identifier"), "got: {}", err.message);
    }

    #[test]
    fn test_parse_call_implicit_alias_rejected() {
        // Implicit alias (Postgres allows `FROM x y`) is intentionally
        // NOT supported by this parser — only `AS y` works.
        let err = parse(
            "SELECT c.name FROM customers.by_owner(42) c WHERE c.name = 'A'",
        ).unwrap_err();
        // the dangling `c` before WHERE is an unexpected identifier token
        assert!(
            err.message.contains("expected") || err.message.contains("got"),
            "got: {}", err.message,
        );
    }
}
