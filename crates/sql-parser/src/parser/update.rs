use crate::ast::*;
use super::core::ParserCore;
use super::token::{ParseError, TokenKind};

pub(crate) fn parse_update(p: &mut ParserCore) -> Result<AstUpdate, ParseError> {
    p.expect(TokenKind::Update)?;
    let (table, _) = p.expect_ident()?;
    p.expect(TokenKind::Set)?;

    let mut assignments = vec![];
    loop {
        // Parse: col = expr  or  table.col = expr
        let (col_or_table, _) = p.expect_ident()?;
        let col = if p.at(&TokenKind::Dot)? {
            p.eat()?;
            let (col, _) = p.expect_ident()?;
            col
        } else {
            col_or_table
        };
        p.expect(TokenKind::Eq)?;
        let expr = p.parse_expr(0)?;
        assignments.push((col, expr));

        if !p.at(&TokenKind::Comma)? {
            break;
        }
        p.eat()?;
    }

    let filter = if p.at(&TokenKind::Where)? {
        p.expect(TokenKind::Where)?;
        Some(p.parse_expr(0)?)
    } else {
        None
    };

    Ok(AstUpdate { table, assignments, filter })
}
