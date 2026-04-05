pub mod token;
mod lexer;

pub use token::{ParseError, Span};

use crate::ast::*;
use lexer::Lexer;
use token::*;

pub fn parse(input: &str) -> Result<AstCreateTable, ParseError> {
    let mut parser = Parser::new(input);
    parser.parse_create_table()
}

struct Parser<'a> {
    #[allow(dead_code)]
    input: &'a str,
    lexer: Lexer<'a>,
    current: Option<Token>,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input,
            lexer: Lexer::new(input),
            current: None,
        }
    }

    fn peek(&mut self) -> Result<&Token, ParseError> {
        if self.current.is_none() {
            self.current = Some(self.lexer.next_token()?);
        }
        Ok(self.current.as_ref().unwrap())
    }

    fn eat(&mut self) -> Result<Token, ParseError> {
        if let Some(tok) = self.current.take() {
            Ok(tok)
        } else {
            self.lexer.next_token()
        }
    }

    fn expect(&mut self, expected: &TokenKind) -> Result<Token, ParseError> {
        let tok = self.eat()?;
        if token_kind_matches(&tok.kind, expected) {
            Ok(tok)
        } else {
            Err(ParseError {
                message: format!(
                    "expected {}, got {}",
                    token_kind_name(expected),
                    token_kind_name(&tok.kind)
                ),
                span: tok.span,
            })
        }
    }

    fn at(&mut self, kind: &TokenKind) -> Result<bool, ParseError> {
        Ok(token_kind_matches(&self.peek()?.kind, kind))
    }

    fn expect_ident(&mut self) -> Result<String, ParseError> {
        let tok = self.eat()?;
        match tok.kind {
            TokenKind::Ident(name) => Ok(name),
            _ => Err(ParseError {
                message: format!("expected identifier, got {}", token_kind_name(&tok.kind)),
                span: tok.span,
            }),
        }
    }

    fn parse_create_table(&mut self) -> Result<AstCreateTable, ParseError> {
        self.expect(&TokenKind::Create)?;
        self.expect(&TokenKind::Table)?;
        let name = self.expect_ident()?;
        self.expect(&TokenKind::LParen)?;

        let mut columns = Vec::new();
        let mut constraints = Vec::new();

        loop {
            if self.at(&TokenKind::Primary)? || self.at(&TokenKind::Index)? {
                constraints.push(self.parse_table_constraint()?);
            } else {
                columns.push(self.parse_column_def()?);
            }

            if self.at(&TokenKind::Comma)? {
                self.eat()?;
            } else {
                break;
            }
        }

        self.expect(&TokenKind::RParen)?;

        if self.at(&TokenKind::Semicolon)? {
            self.eat()?;
        }

        self.expect(&TokenKind::Eof)?;

        Ok(AstCreateTable {
            name,
            columns,
            constraints,
        })
    }

    fn parse_column_def(&mut self) -> Result<AstColumnDef, ParseError> {
        let name = self.expect_ident()?;
        let data_type = self.parse_data_type()?;

        let mut not_null = false;
        let mut primary_key = false;

        loop {
            if self.at(&TokenKind::Not)? {
                self.eat()?;
                self.expect(&TokenKind::Null)?;
                not_null = true;
            } else if self.at(&TokenKind::Primary)? {
                self.eat()?;
                self.expect(&TokenKind::Key)?;
                primary_key = true;
                not_null = true;
            } else {
                break;
            }
        }

        Ok(AstColumnDef {
            name,
            data_type,
            not_null,
            primary_key,
        })
    }

    fn parse_data_type(&mut self) -> Result<AstDataType, ParseError> {
        let tok = self.eat()?;
        match tok.kind {
            TokenKind::KwI64 => Ok(AstDataType::I64),
            TokenKind::KwString => Ok(AstDataType::String),
            _ => Err(ParseError {
                message: format!("expected data type, got {}", token_kind_name(&tok.kind)),
                span: tok.span,
            }),
        }
    }

    fn parse_table_constraint(&mut self) -> Result<AstTableConstraint, ParseError> {
        if self.at(&TokenKind::Primary)? {
            self.eat()?;
            self.expect(&TokenKind::Key)?;
            let columns = self.parse_ident_list()?;
            Ok(AstTableConstraint::PrimaryKey { columns })
        } else {
            self.expect(&TokenKind::Index)?;

            let name = if self.at(&TokenKind::LParen)? {
                None
            } else {
                Some(self.expect_ident()?)
            };

            let columns = self.parse_ident_list()?;

            let index_type = if self.at(&TokenKind::Using)? {
                self.eat()?;
                if self.at(&TokenKind::BTree)? {
                    self.eat()?;
                    AstIndexType::BTree
                } else if self.at(&TokenKind::Hash)? {
                    self.eat()?;
                    AstIndexType::Hash
                } else {
                    let tok = self.eat()?;
                    return Err(ParseError {
                        message: format!("expected BTREE or HASH, got {}", token_kind_name(&tok.kind)),
                        span: tok.span,
                    });
                }
            } else {
                AstIndexType::BTree
            };

            Ok(AstTableConstraint::Index { name, columns, index_type })
        }
    }

    fn parse_ident_list(&mut self) -> Result<Vec<String>, ParseError> {
        self.expect(&TokenKind::LParen)?;
        let mut names = vec![self.expect_ident()?];
        while self.at(&TokenKind::Comma)? {
            self.eat()?;
            names.push(self.expect_ident()?);
        }
        self.expect(&TokenKind::RParen)?;
        Ok(names)
    }
}

fn token_kind_matches(a: &TokenKind, b: &TokenKind) -> bool {
    std::mem::discriminant(a) == std::mem::discriminant(b)
}

fn token_kind_name(kind: &TokenKind) -> &str {
    match kind {
        TokenKind::Create => "CREATE",
        TokenKind::Table => "TABLE",
        TokenKind::Index => "INDEX",
        TokenKind::Primary => "PRIMARY",
        TokenKind::Key => "KEY",
        TokenKind::Not => "NOT",
        TokenKind::Null => "NULL",
        TokenKind::Using => "USING",
        TokenKind::BTree => "BTREE",
        TokenKind::Hash => "HASH",
        TokenKind::KwI64 => "I64",
        TokenKind::KwString => "STRING",
        TokenKind::Ident(_) => "identifier",
        TokenKind::LParen => "'('",
        TokenKind::RParen => "')'",
        TokenKind::Comma => "','",
        TokenKind::Semicolon => "';'",
        TokenKind::Eof => "end of input",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_table() {
        let ast = parse("CREATE TABLE users (id I64, name STRING)").unwrap();
        assert_eq!(ast.name, "users");
        assert_eq!(ast.columns.len(), 2);
        assert_eq!(ast.columns[0].name, "id");
        assert_eq!(ast.columns[0].data_type, AstDataType::I64);
        assert!(!ast.columns[0].not_null);
        assert_eq!(ast.columns[1].name, "name");
        assert_eq!(ast.columns[1].data_type, AstDataType::String);
    }

    #[test]
    fn test_not_null() {
        let ast = parse("CREATE TABLE t (name STRING NOT NULL)").unwrap();
        assert!(ast.columns[0].not_null);
    }

    #[test]
    fn test_inline_primary_key() {
        let ast = parse("CREATE TABLE t (id I64 NOT NULL PRIMARY KEY, name STRING)").unwrap();
        assert!(ast.columns[0].primary_key);
        assert!(ast.columns[0].not_null);
        assert!(!ast.columns[1].primary_key);
    }

    #[test]
    fn test_primary_key_implies_not_null() {
        let ast = parse("CREATE TABLE t (id I64 PRIMARY KEY)").unwrap();
        assert!(ast.columns[0].primary_key);
        assert!(ast.columns[0].not_null);
    }

    #[test]
    fn test_constraint_primary_key() {
        let ast = parse("CREATE TABLE t (a I64, b I64, PRIMARY KEY (a, b))").unwrap();
        assert_eq!(ast.constraints.len(), 1);
        match &ast.constraints[0] {
            AstTableConstraint::PrimaryKey { columns } => {
                assert_eq!(columns, &["a", "b"]);
            }
            _ => panic!("expected PrimaryKey constraint"),
        }
    }

    #[test]
    fn test_index_with_name() {
        let ast = parse("CREATE TABLE t (id I64, name STRING, INDEX idx_name (name))").unwrap();
        assert_eq!(ast.constraints.len(), 1);
        match &ast.constraints[0] {
            AstTableConstraint::Index { name, columns, index_type } => {
                assert_eq!(name.as_deref(), Some("idx_name"));
                assert_eq!(columns, &["name"]);
                assert_eq!(*index_type, AstIndexType::BTree); // default
            }
            _ => panic!("expected Index constraint"),
        }
    }

    #[test]
    fn test_index_without_name() {
        let ast = parse("CREATE TABLE t (id I64, INDEX (id))").unwrap();
        match &ast.constraints[0] {
            AstTableConstraint::Index { name, columns, index_type } => {
                assert!(name.is_none());
                assert_eq!(columns, &["id"]);
                assert_eq!(*index_type, AstIndexType::BTree); // default
            }
            _ => panic!("expected Index constraint"),
        }
    }

    #[test]
    fn test_index_using_btree() {
        let ast = parse("CREATE TABLE t (id I64, INDEX idx_id (id) USING BTREE)").unwrap();
        match &ast.constraints[0] {
            AstTableConstraint::Index { index_type, .. } => {
                assert_eq!(*index_type, AstIndexType::BTree);
            }
            _ => panic!("expected Index constraint"),
        }
    }

    #[test]
    fn test_index_using_hash() {
        let ast = parse("CREATE TABLE t (id I64, INDEX idx_id (id) USING HASH)").unwrap();
        match &ast.constraints[0] {
            AstTableConstraint::Index { index_type, .. } => {
                assert_eq!(*index_type, AstIndexType::Hash);
            }
            _ => panic!("expected Index constraint"),
        }
    }

    #[test]
    fn test_composite_index() {
        let ast = parse("CREATE TABLE t (a I64, b STRING, INDEX idx_ab (a, b))").unwrap();
        match &ast.constraints[0] {
            AstTableConstraint::Index { columns, .. } => {
                assert_eq!(columns, &["a", "b"]);
            }
            _ => panic!("expected Index constraint"),
        }
    }

    #[test]
    fn test_all_data_types() {
        let ast = parse(
            "CREATE TABLE t (a STRING, b I64)"
        ).unwrap();
        assert_eq!(ast.columns[0].data_type, AstDataType::String);
        assert_eq!(ast.columns[1].data_type, AstDataType::I64);
    }

    #[test]
    fn test_trailing_semicolon() {
        let ast = parse("CREATE TABLE t (id I64);").unwrap();
        assert_eq!(ast.name, "t");
    }

    #[test]
    fn test_case_insensitive() {
        let ast = parse("create table Users (Id i64 not null primary key)").unwrap();
        assert_eq!(ast.name, "Users");
        assert_eq!(ast.columns[0].name, "Id");
        assert!(ast.columns[0].primary_key);
    }

    #[test]
    fn test_full_example() {
        let sql = "
            CREATE TABLE users (
                id I64 NOT NULL PRIMARY KEY,
                name STRING NOT NULL,
                age I64,
                email STRING,
                INDEX idx_email (email),
                INDEX idx_name_age (name, age)
            );
        ";
        let ast = parse(sql).unwrap();
        assert_eq!(ast.name, "users");
        assert_eq!(ast.columns.len(), 4);
        assert_eq!(ast.constraints.len(), 2);

        assert!(ast.columns[0].primary_key);
        assert!(ast.columns[0].not_null);
        assert!(ast.columns[1].not_null);
        assert!(!ast.columns[2].not_null);
        assert!(!ast.columns[3].not_null);
    }

    #[test]
    fn test_error_missing_type() {
        let err = parse("CREATE TABLE t (id)").unwrap_err();
        assert!(err.message.contains("expected data type"));
    }

    #[test]
    fn test_error_missing_table_name() {
        let err = parse("CREATE TABLE (id I64)").unwrap_err();
        assert!(err.message.contains("expected identifier"));
    }

    #[test]
    fn test_error_render() {
        let input = "CREATE TABLE t (id)";
        let err = parse(input).unwrap_err();
        let rendered = err.render(input);
        assert!(rendered.contains("parse error:"));
        assert!(rendered.contains("^"));
    }
}
