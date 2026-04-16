use crate::ast::*;
use super::lexer::Lexer;
use super::token::{ParseError, Span, Token, TokenKind};

pub(crate) struct ParserCore<'a> {
    lexer: Lexer<'a>,
    current: Token,
    has_current: bool,
}

impl<'a> ParserCore<'a> {
    pub fn new(input: &'a str) -> Self {
        Self {
            lexer: Lexer::new(input),
            current: Token {
                kind: TokenKind::Eof,
                span: Span { offset: 0, len: 0 },
            },
            has_current: false,
        }
    }

    pub fn peek(&mut self) -> Result<&Token, ParseError> {
        if !self.has_current {
            self.current = self.lexer.next_token()?;
            self.has_current = true;
        }
        Ok(&self.current)
    }

    pub fn eat(&mut self) -> Result<Token, ParseError> {
        self.peek()?;
        self.has_current = false;
        Ok(self.current.clone())
    }

    pub fn at(&mut self, expected: &TokenKind) -> Result<bool, ParseError> {
        Ok(self.peek()?.kind.matches(expected))
    }

    pub fn expect(&mut self, expected: TokenKind) -> Result<Token, ParseError> {
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

    pub fn expect_ident(&mut self) -> Result<(String, Span), ParseError> {
        let tok = self.eat()?;
        match tok.kind {
            TokenKind::Ident(name) => Ok((name, tok.span)),
            _ => Err(ParseError::new(
                format!("expected identifier, got {}", tok.kind.name()),
                tok.span,
            )),
        }
    }

    pub fn expect_eof(&mut self) -> Result<(), ParseError> {
        self.expect(TokenKind::Eof)?;
        Ok(())
    }

    /// Check if the current token is an identifier matching `kw` (case-insensitive).
    pub fn at_keyword(&mut self, kw: &str) -> Result<bool, ParseError> {
        match &self.peek()?.kind {
            TokenKind::Ident(name) => Ok(name.eq_ignore_ascii_case(kw)),
            _ => Ok(false),
        }
    }

    /// Consume the current token if it is an identifier matching `kw` (case-insensitive).
    pub fn expect_keyword(&mut self, kw: &str) -> Result<Token, ParseError> {
        let tok = self.eat()?;
        match &tok.kind {
            TokenKind::Ident(name) if name.eq_ignore_ascii_case(kw) => Ok(tok),
            _ => Err(ParseError::new(
                format!("expected {kw}, got {}", tok.kind.name()),
                tok.span,
            )),
        }
    }

    // ── Expression parsing (Pratt) ──────────────────────────────────────

    pub fn parse_expr(&mut self, min_prec: u8) -> Result<AstExpr, ParseError> {
        let mut left = self.parse_atom()?;

        loop {
            if self.at(&TokenKind::In)? {
                self.eat()?;
                self.expect(TokenKind::LParen)?;
                if self.at(&TokenKind::Select)? {
                    let subquery = super::select::parse_select_inner(self)?;
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
            self.eat()?;
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

            TokenKind::Reactive => {
                self.eat()?;
                self.expect(TokenKind::LParen)?;
                let expr = self.parse_expr(0)?;
                self.expect(TokenKind::RParen)?;
                Ok(AstExpr::Reactive(Box::new(expr)))
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
                    let subquery = super::select::parse_select_inner(self)?;
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
