use crate::ast::*;
use super::core::ParserCore;
use super::token::{ParseError, TokenKind};

pub(crate) fn parse_create_table(p: &mut ParserCore) -> Result<AstCreateTable, ParseError> {
    p.expect(TokenKind::Create)?;
    p.expect_keyword("TABLE")?;
    let (name, _) = p.expect_ident()?;
    p.expect(TokenKind::LParen)?;

    let mut columns = Vec::new();
    let mut constraints = Vec::new();

    loop {
        if p.at_keyword("PRIMARY")? || p.at_keyword("INDEX")? {
            constraints.push(parse_table_constraint(p)?);
        } else {
            columns.push(parse_column_def(p)?);
        }

        if p.at(&TokenKind::Comma)? {
            p.eat()?;
        } else {
            break;
        }
    }

    p.expect(TokenKind::RParen)?;

    Ok(AstCreateTable {
        name,
        columns,
        constraints,
    })
}

fn parse_column_def(p: &mut ParserCore) -> Result<AstColumnDef, ParseError> {
    let (name, _) = p.expect_ident()?;
    let data_type = parse_data_type(p)?;

    let mut not_null = false;
    let mut primary_key = false;

    loop {
        if p.at(&TokenKind::Not)? {
            p.eat()?;
            p.expect(TokenKind::Null)?;
            not_null = true;
        } else if p.at_keyword("PRIMARY")? {
            p.eat()?;
            p.expect_keyword("KEY")?;
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

fn parse_data_type(p: &mut ParserCore) -> Result<AstDataType, ParseError> {
    if p.at_keyword("I64")? {
        p.eat()?;
        Ok(AstDataType::I64)
    } else if p.at_keyword("STRING")? {
        p.eat()?;
        Ok(AstDataType::String)
    } else {
        let tok = p.eat()?;
        Err(ParseError::new(
            format!("expected data type, got {}", tok.kind.name()),
            tok.span,
        ))
    }
}

fn parse_table_constraint(p: &mut ParserCore) -> Result<AstTableConstraint, ParseError> {
    if p.at_keyword("PRIMARY")? {
        p.eat()?;
        p.expect_keyword("KEY")?;
        let columns = parse_ident_list(p)?;
        Ok(AstTableConstraint::PrimaryKey { columns })
    } else {
        p.expect_keyword("INDEX")?;

        let name = if p.at(&TokenKind::LParen)? {
            None
        } else {
            let (name, _) = p.expect_ident()?;
            Some(name)
        };

        let columns = parse_ident_list(p)?;

        let index_type = if p.at_keyword("USING")? {
            p.eat()?;
            if p.at_keyword("BTREE")? {
                p.eat()?;
                AstIndexType::BTree
            } else if p.at_keyword("HASH")? {
                p.eat()?;
                AstIndexType::Hash
            } else {
                let tok = p.eat()?;
                return Err(ParseError::new(
                    format!("expected BTREE or HASH, got {}", tok.kind.name()),
                    tok.span,
                ));
            }
        } else {
            AstIndexType::BTree
        };

        Ok(AstTableConstraint::Index { name, columns, index_type })
    }
}

fn parse_ident_list(p: &mut ParserCore) -> Result<Vec<String>, ParseError> {
    p.expect(TokenKind::LParen)?;
    let (first, _) = p.expect_ident()?;
    let mut names = vec![first];
    while p.at(&TokenKind::Comma)? {
        p.eat()?;
        let (name, _) = p.expect_ident()?;
        names.push(name);
    }
    p.expect(TokenKind::RParen)?;
    Ok(names)
}
