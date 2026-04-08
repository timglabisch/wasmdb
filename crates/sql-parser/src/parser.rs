mod core;
mod create_table;
mod insert;
mod lexer;
pub(crate) mod select;
pub mod token;

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
        TokenKind::Create => Ok(Statement::CreateTable(create_table::parse_create_table(p)?)),
        _ => {
            let tok = p.peek()?.clone();
            Err(ParseError::new(
                format!("expected SELECT, INSERT or CREATE, got {}", tok.kind.name()),
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
        assert_eq!(ast.sources[0].table, "users");
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
        assert_eq!(ast.sources[1].table, "orders");
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
        assert_eq!(ast.sources[0].table, "users");
        assert_eq!(ast.sources[1].table, "orders");
        assert_eq!(ast.sources[2].table, "products");
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

    // ── INVALIDATE_ON tests ─────────────────────────────────────────────

    #[test]
    fn test_parse_invalidate_on_simple() {
        let ast = parse("SELECT INVALIDATE_ON(users.id = :uid) AS inv, users.name FROM users").unwrap();
        assert_eq!(ast.result_columns.len(), 2);
        assert!(matches!(&ast.result_columns[0].expr, AstExpr::InvalidateOn(_)));
        assert_eq!(ast.result_columns[0].alias, Some("inv".to_string()));
        if let AstExpr::InvalidateOn(inner) = &ast.result_columns[0].expr {
            assert!(matches!(inner.as_ref(), AstExpr::Binary { op: Operator::Eq, .. }));
        }
    }

    #[test]
    fn test_parse_invalidate_on_compound() {
        let ast = parse(
            "SELECT INVALIDATE_ON(users.id = :uid AND users.age > 18) FROM users"
        ).unwrap();
        if let AstExpr::InvalidateOn(inner) = &ast.result_columns[0].expr {
            assert!(matches!(inner.as_ref(), AstExpr::Binary { op: Operator::And, .. }));
        } else {
            panic!("expected InvalidateOn");
        }
    }

    #[test]
    fn test_parse_invalidate_on_case_insensitive() {
        let ast = parse("SELECT invalidate_on(users.id = :uid) FROM users").unwrap();
        assert!(matches!(&ast.result_columns[0].expr, AstExpr::InvalidateOn(_)));
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
}
