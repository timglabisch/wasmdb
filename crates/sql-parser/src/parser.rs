mod lexer;
pub mod token;

use crate::ast::*;
use lexer::Lexer;
pub use token::{ParseError, Span, Token, TokenKind};

// ── Parser ───────────────���──────────────────────────────────────────────────

pub struct Parser<'a> {
    lexer: Lexer<'a>,
    current: Token,
    has_current: bool,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            lexer: Lexer::new(input),
            current: Token {
                kind: TokenKind::Eof,
                span: Span { offset: 0, len: 0 },
            },
            has_current: false,
        }
    }

    fn peek(&mut self) -> Result<&Token, ParseError> {
        if !self.has_current {
            self.current = self.lexer.next_token()?;
            self.has_current = true;
        }
        Ok(&self.current)
    }

    fn eat(&mut self) -> Result<Token, ParseError> {
        self.peek()?;
        self.has_current = false;
        Ok(self.current.clone())
    }

    fn at(&mut self, expected: &TokenKind) -> Result<bool, ParseError> {
        Ok(self.peek()?.kind.matches(expected))
    }

    fn expect(&mut self, expected: TokenKind) -> Result<Token, ParseError> {
        let tok = self.eat()?;
        if tok.kind.matches(&expected) {
            Ok(tok)
        } else {
            Err(ParseError::new(
                format!("expected {}, got {}", expected.name(), tok.kind.name()),
                tok.span,
            ))
        }
    }

    fn expect_ident(&mut self) -> Result<(String, Span), ParseError> {
        let tok = self.eat()?;
        match tok.kind {
            TokenKind::Ident(name) => Ok((name, tok.span)),
            _ => Err(ParseError::new(
                format!("expected identifier, got {}", tok.kind.name()),
                tok.span,
            )),
        }
    }

    // ── Statement parsing ────────────────���──────────────────────────────

    /// Parse a SELECT statement without consuming EOF (used for subqueries).
    fn parse_select_inner(&mut self) -> Result<AstSelect, ParseError> {
        self.expect(TokenKind::Select)?;
        let result_columns = self.parse_result_columns()?;
        self.expect(TokenKind::From)?;
        let sources = self.parse_sources()?;

        let filter = if self.at(&TokenKind::Where)? {
            self.parse_where()?
        } else {
            vec![]
        };

        let group_by = if self.at(&TokenKind::Group)? {
            self.parse_group_by()?
        } else {
            vec![]
        };

        let order_by = if self.at(&TokenKind::Order)? {
            self.parse_order_by()?
        } else {
            vec![]
        };

        let limit = if self.at(&TokenKind::Limit)? {
            self.eat()?;
            let tok = self.peek()?.clone();
            match &tok.kind {
                TokenKind::Integer(_) => {
                    let tok = self.eat()?;
                    match tok.kind {
                        TokenKind::Integer(n) => Some(AstLimit::Value(n as u64)),
                        _ => unreachable!(),
                    }
                }
                TokenKind::Placeholder(_) => {
                    let tok = self.eat()?;
                    match tok.kind {
                        TokenKind::Placeholder(name) => Some(AstLimit::Placeholder(name)),
                        _ => unreachable!(),
                    }
                }
                _ => return Err(ParseError::new(
                    format!("expected integer or placeholder after LIMIT, got {}", tok.kind.name()),
                    tok.span,
                )),
            }
        } else {
            None
        };

        Ok(AstSelect {
            sources,
            filter,
            group_by,
            order_by,
            limit,
            result_columns,
        })
    }

    fn parse_select(&mut self) -> Result<AstSelect, ParseError> {
        let select = self.parse_select_inner()?;
        self.expect(TokenKind::Eof)?;
        Ok(select)
    }

    fn parse_result_columns(&mut self) -> Result<Vec<AstResultColumn>, ParseError> {
        let mut columns = vec![self.parse_result_column()?];
        while self.at(&TokenKind::Comma)? {
            self.eat()?;
            columns.push(self.parse_result_column()?);
        }
        Ok(columns)
    }

    fn parse_result_column(&mut self) -> Result<AstResultColumn, ParseError> {
        let expr = self.parse_expr(0)?;
        let alias = if self.at(&TokenKind::As)? {
            self.eat()?;
            let (name, _) = self.expect_ident()?;
            Some(name)
        } else {
            None
        };
        Ok(AstResultColumn { expr, alias })
    }

    fn parse_sources(&mut self) -> Result<Vec<AstSourceEntry>, ParseError> {
        let (table, _) = self.expect_ident()?;
        let mut sources = vec![AstSourceEntry { table, join: None }];

        loop {
            if self.at(&TokenKind::Inner)?
                || self.at(&TokenKind::Left)?
                || self.at(&TokenKind::Join)?
            {
                sources.push(self.parse_join()?);
            } else {
                break;
            }
        }

        Ok(sources)
    }

    fn parse_join(&mut self) -> Result<AstSourceEntry, ParseError> {
        let join_type = if self.at(&TokenKind::Inner)? {
            self.eat()?;
            JoinType::Inner
        } else if self.at(&TokenKind::Left)? {
            self.eat()?;
            JoinType::Left
        } else {
            JoinType::Inner
        };

        self.expect(TokenKind::Join)?;
        let (table, _) = self.expect_ident()?;
        self.expect(TokenKind::On)?;
        let on_expr = self.parse_expr(0)?;

        Ok(AstSourceEntry {
            table,
            join: Some(AstJoinClause {
                join_type,
                on: vec![on_expr],
            }),
        })
    }

    fn parse_where(&mut self) -> Result<Vec<AstExpr>, ParseError> {
        self.expect(TokenKind::Where)?;
        let expr = self.parse_expr(0)?;
        Ok(vec![expr])
    }

    fn parse_group_by(&mut self) -> Result<Vec<AstExpr>, ParseError> {
        self.expect(TokenKind::Group)?;
        self.expect(TokenKind::By)?;
        let mut exprs = vec![self.parse_expr(0)?];
        while self.at(&TokenKind::Comma)? {
            self.eat()?;
            exprs.push(self.parse_expr(0)?);
        }
        Ok(exprs)
    }

    fn parse_order_by(&mut self) -> Result<Vec<AstOrderSpec>, ParseError> {
        self.expect(TokenKind::Order)?;
        self.expect(TokenKind::By)?;
        let mut specs = vec![self.parse_order_spec()?];
        while self.at(&TokenKind::Comma)? {
            self.eat()?;
            specs.push(self.parse_order_spec()?);
        }
        Ok(specs)
    }

    fn parse_order_spec(&mut self) -> Result<AstOrderSpec, ParseError> {
        let expr = self.parse_expr(0)?;
        let direction = if self.at(&TokenKind::Desc)? {
            self.eat()?;
            OrderDirection::Desc
        } else if self.at(&TokenKind::Asc)? {
            self.eat()?;
            OrderDirection::Asc
        } else {
            OrderDirection::Asc
        };
        Ok(AstOrderSpec { expr, direction })
    }

    // ── Expression parsing (Pratt) ──────────────────────────────────────

    fn parse_expr(&mut self, min_prec: u8) -> Result<AstExpr, ParseError> {
        let mut left = self.parse_atom()?;

        loop {
            // IN has higher precedence than any binary operator.
            if self.at(&TokenKind::In)? {
                self.eat()?;
                self.expect(TokenKind::LParen)?;
                if self.at(&TokenKind::Select)? {
                    let subquery = self.parse_select_inner()?;
                    self.expect(TokenKind::RParen)?;
                    left = AstExpr::InSubquery {
                        expr: Box::new(left),
                        subquery: Box::new(subquery),
                    };
                } else {
                    let mut values = vec![self.parse_expr(0)?];
                    while self.at(&TokenKind::Comma)? {
                        self.eat()?;
                        values.push(self.parse_expr(0)?);
                    }
                    self.expect(TokenKind::RParen)?;
                    left = AstExpr::InList {
                        expr: Box::new(left),
                        values,
                    };
                }
                continue;
            }

            let Some((op, prec)) = self.peek_operator()? else {
                break;
            };
            if prec < min_prec {
                break;
            }
            self.eat()?; // consume operator token
            let right = self.parse_expr(prec + 1)?;
            left = AstExpr::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn peek_operator(&mut self) -> Result<Option<(Operator, u8)>, ParseError> {
        let kind = &self.peek()?.kind;
        let result = match kind {
            TokenKind::Or => Some((Operator::Or, 1)),
            TokenKind::And => Some((Operator::And, 2)),
            TokenKind::Eq => Some((Operator::Eq, 3)),
            TokenKind::Neq => Some((Operator::Neq, 3)),
            TokenKind::Lt => Some((Operator::Lt, 3)),
            TokenKind::Gt => Some((Operator::Gt, 3)),
            TokenKind::Lte => Some((Operator::Lte, 3)),
            TokenKind::Gte => Some((Operator::Gte, 3)),
            _ => None,
        };
        Ok(result)
    }

    fn parse_atom(&mut self) -> Result<AstExpr, ParseError> {
        let tok = self.peek()?.clone();
        match &tok.kind {
            TokenKind::Integer(_)
            | TokenKind::Float(_)
            | TokenKind::Str(_)
            | TokenKind::True
            | TokenKind::False
            | TokenKind::Null
            | TokenKind::Placeholder(_) => {
                let tok = self.eat()?;
                Ok(token_to_literal(tok))
            }

            TokenKind::InvalidateOn => {
                self.eat()?;
                self.expect(TokenKind::LParen)?;
                let expr = self.parse_expr(0)?;
                self.expect(TokenKind::RParen)?;
                Ok(AstExpr::InvalidateOn(Box::new(expr)))
            }

            TokenKind::Count | TokenKind::Sum | TokenKind::Min | TokenKind::Max => {
                let func_tok = self.eat()?;
                let func = match func_tok.kind {
                    TokenKind::Count => AggFunc::Count,
                    TokenKind::Sum => AggFunc::Sum,
                    TokenKind::Min => AggFunc::Min,
                    TokenKind::Max => AggFunc::Max,
                    _ => unreachable!(),
                };
                self.expect(TokenKind::LParen)?;
                let arg = self.parse_expr(0)?;
                self.expect(TokenKind::RParen)?;
                Ok(AstExpr::Aggregate {
                    func,
                    arg: Box::new(arg),
                })
            }

            TokenKind::Ident(_) => {
                let (table, _) = self.expect_ident()?;
                self.expect(TokenKind::Dot)?;
                let (column, _) = self.expect_ident()?;
                Ok(AstExpr::Column(AstColumnRef { table, column }))
            }

            TokenKind::LParen => {
                self.eat()?;
                if self.at(&TokenKind::Select)? {
                    let subquery = self.parse_select_inner()?;
                    self.expect(TokenKind::RParen)?;
                    Ok(AstExpr::Subquery(Box::new(subquery)))
                } else {
                    let expr = self.parse_expr(0)?;
                    self.expect(TokenKind::RParen)?;
                    Ok(expr)
                }
            }

            _ => Err(ParseError::new(
                format!("expected expression, got {}", tok.kind.name()),
                tok.span,
            )),
        }
    }
}

fn token_to_literal(tok: Token) -> AstExpr {
    match tok.kind {
        TokenKind::Integer(n) => AstExpr::Literal(Value::Int(n)),
        TokenKind::Float(f) => AstExpr::Literal(Value::Float(f)),
        TokenKind::Str(s) => AstExpr::Literal(Value::Text(s)),
        TokenKind::True => AstExpr::Literal(Value::Bool(true)),
        TokenKind::False => AstExpr::Literal(Value::Bool(false)),
        TokenKind::Null => AstExpr::Literal(Value::Null),
        TokenKind::Placeholder(name) => AstExpr::Literal(Value::Placeholder(name)),
        _ => unreachable!(),
    }
}

// ── Public API ───���────────────────────────────────────────��─────────────────

pub fn parse(input: &str) -> Result<AstSelect, ParseError> {
    let mut parser = Parser::new(input);
    parser.parse_select()
}

// ── Tests ──────���───────────────────────────────��────────────────────────────

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
        // AND binds tighter than OR: a OR b AND c → OR(a, AND(b, c))
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

    // ── IN + Subquery tests ────���───────────────────────────────────────

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

    #[test]
    fn test_parse_placeholder_on_left_side() {
        let ast = parse("SELECT u.x FROM u WHERE :val < u.age").unwrap();
        assert!(matches!(
            &ast.filter[0],
            AstExpr::Binary { op: Operator::Lt, left, .. }
            if matches!(left.as_ref(), AstExpr::Literal(Value::Placeholder(name)) if name == "val")
        ));
    }
}
