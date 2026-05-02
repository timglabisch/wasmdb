use crate::ast::*;
use super::core::ParserCore;
use super::token::{ParseError, TokenKind};

pub(crate) fn parse_delete(p: &mut ParserCore) -> Result<AstDelete, ParseError> {
    p.expect(TokenKind::Delete)?;
    p.expect(TokenKind::From)?;
    let (table, _) = p.expect_ident()?;

    let filter = if p.at(&TokenKind::Where)? {
        p.expect(TokenKind::Where)?;
        Some(p.parse_expr(0)?)
    } else {
        None
    };

    Ok(AstDelete { table, filter })
}
