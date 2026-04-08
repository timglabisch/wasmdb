use crate::ast::*;
use super::core::ParserCore;
use super::token::{ParseError, TokenKind};

pub(crate) fn parse_insert(p: &mut ParserCore) -> Result<AstInsert, ParseError> {
    p.expect(TokenKind::Insert)?;
    p.expect(TokenKind::Into)?;
    let (table, _) = p.expect_ident()?;

    let columns = if p.at(&TokenKind::LParen)? {
        p.eat()?;
        let mut cols = vec![];
        let (name, _) = p.expect_ident()?;
        cols.push(name);
        while p.at(&TokenKind::Comma)? {
            p.eat()?;
            let (name, _) = p.expect_ident()?;
            cols.push(name);
        }
        p.expect(TokenKind::RParen)?;
        cols
    } else {
        vec![]
    };

    p.expect(TokenKind::Values)?;
    let mut values = vec![parse_value_row(p)?];
    while p.at(&TokenKind::Comma)? {
        p.eat()?;
        values.push(parse_value_row(p)?);
    }

    p.expect_eof()?;
    Ok(AstInsert { table, columns, values })
}

fn parse_value_row(p: &mut ParserCore) -> Result<Vec<AstExpr>, ParseError> {
    p.expect(TokenKind::LParen)?;
    let mut exprs = vec![p.parse_expr(0)?];
    while p.at(&TokenKind::Comma)? {
        p.eat()?;
        exprs.push(p.parse_expr(0)?);
    }
    p.expect(TokenKind::RParen)?;
    Ok(exprs)
}
