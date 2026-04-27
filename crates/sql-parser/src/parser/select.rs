use crate::ast::*;
use super::core::ParserCore;
use super::token::{ParseError, TokenKind};

/// Parse SELECT body without consuming EOF (used for subqueries).
pub(crate) fn parse_select_inner(p: &mut ParserCore) -> Result<AstSelect, ParseError> {
    p.expect(TokenKind::Select)?;
    let result_columns = parse_result_columns(p)?;
    p.expect(TokenKind::From)?;
    let sources = parse_sources(p)?;

    let filter = if p.at(&TokenKind::Where)? {
        parse_where(p)?
    } else {
        vec![]
    };

    let group_by = if p.at(&TokenKind::Group)? {
        parse_group_by(p)?
    } else {
        vec![]
    };

    let order_by = if p.at(&TokenKind::Order)? {
        parse_order_by(p)?
    } else {
        vec![]
    };

    let limit = if p.at(&TokenKind::Limit)? {
        p.eat()?;
        let tok = p.peek()?.clone();
        match &tok.kind {
            TokenKind::Integer(_) => {
                let tok = p.eat()?;
                match tok.kind {
                    TokenKind::Integer(n) => Some(AstLimit::Value(n as u64)),
                    _ => unreachable!(),
                }
            }
            TokenKind::Placeholder(_) => {
                let tok = p.eat()?;
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

/// Parse a full SELECT statement (inner + EOF).
pub(crate) fn parse_select(p: &mut ParserCore) -> Result<AstSelect, ParseError> {
    let select = parse_select_inner(p)?;
    p.expect_eof()?;
    Ok(select)
}

fn parse_result_columns(p: &mut ParserCore) -> Result<Vec<AstResultColumn>, ParseError> {
    let mut columns = vec![parse_result_column(p)?];
    while p.at(&TokenKind::Comma)? {
        p.eat()?;
        columns.push(parse_result_column(p)?);
    }
    Ok(columns)
}

fn parse_result_column(p: &mut ParserCore) -> Result<AstResultColumn, ParseError> {
    let expr = p.parse_expr(0)?;
    let alias = if p.at(&TokenKind::As)? {
        p.eat()?;
        let (name, _) = p.expect_ident()?;
        Some(name)
    } else {
        None
    };
    Ok(AstResultColumn { expr, alias })
}

fn parse_sources(p: &mut ParserCore) -> Result<Vec<AstSourceEntry>, ParseError> {
    let mut sources = vec![parse_source_entry(p)?];

    loop {
        if p.at(&TokenKind::Inner)?
            || p.at(&TokenKind::Left)?
            || p.at(&TokenKind::Join)?
        {
            sources.push(parse_join(p)?);
        } else {
            break;
        }
    }

    Ok(sources)
}

fn parse_join(p: &mut ParserCore) -> Result<AstSourceEntry, ParseError> {
    let join_type = if p.at(&TokenKind::Inner)? {
        p.eat()?;
        JoinType::Inner
    } else if p.at(&TokenKind::Left)? {
        p.eat()?;
        JoinType::Left
    } else {
        JoinType::Inner
    };

    p.expect(TokenKind::Join)?;
    let entry = parse_source_entry(p)?;
    p.expect(TokenKind::On)?;
    let on_expr = p.parse_expr(0)?;

    Ok(AstSourceEntry {
        join: Some(AstJoinClause {
            join_type,
            on: vec![on_expr],
        }),
        ..entry
    })
}

/// Parse a single FROM-clause source: a plain `table`, optionally
/// followed by `AS alias`. `join` is always `None` here — callers set
/// it for joined entries.
fn parse_source_entry(p: &mut ParserCore) -> Result<AstSourceEntry, ParseError> {
    let (first, _) = p.expect_ident()?;
    let source = AstSource::Table(first);

    let alias = if p.at(&TokenKind::As)? {
        p.eat()?;
        let (name, _) = p.expect_ident()?;
        Some(name)
    } else {
        None
    };

    Ok(AstSourceEntry { source, alias, join: None })
}

fn parse_where(p: &mut ParserCore) -> Result<Vec<AstExpr>, ParseError> {
    p.expect(TokenKind::Where)?;
    let expr = p.parse_expr(0)?;
    Ok(vec![expr])
}

fn parse_group_by(p: &mut ParserCore) -> Result<Vec<AstExpr>, ParseError> {
    p.expect(TokenKind::Group)?;
    p.expect(TokenKind::By)?;
    let mut exprs = vec![p.parse_expr(0)?];
    while p.at(&TokenKind::Comma)? {
        p.eat()?;
        exprs.push(p.parse_expr(0)?);
    }
    Ok(exprs)
}

fn parse_order_by(p: &mut ParserCore) -> Result<Vec<AstOrderSpec>, ParseError> {
    p.expect(TokenKind::Order)?;
    p.expect(TokenKind::By)?;
    let mut specs = vec![parse_order_spec(p)?];
    while p.at(&TokenKind::Comma)? {
        p.eat()?;
        specs.push(parse_order_spec(p)?);
    }
    Ok(specs)
}

fn parse_order_spec(p: &mut ParserCore) -> Result<AstOrderSpec, ParseError> {
    let expr = p.parse_expr(0)?;
    let direction = if p.at(&TokenKind::Desc)? {
        p.eat()?;
        OrderDirection::Desc
    } else if p.at(&TokenKind::Asc)? {
        p.eat()?;
        OrderDirection::Asc
    } else {
        OrderDirection::Asc
    };
    Ok(AstOrderSpec { expr, direction })
}
