use super::token::*;

pub struct Lexer<'a> {
    input: &'a str,
    offset: usize,
}

impl<'a> Lexer<'a> {
    pub fn new(input: &'a str) -> Self {
        Self { input, offset: 0 }
    }

    fn peek_char(&self) -> Option<char> {
        self.input[self.offset..].chars().next()
    }

    fn eat_char(&mut self) -> Option<char> {
        let ch = self.peek_char()?;
        self.offset += ch.len_utf8();
        Some(ch)
    }

    fn eat_while(&mut self, pred: fn(char) -> bool) {
        while let Some(ch) = self.peek_char() {
            if pred(ch) {
                self.offset += ch.len_utf8();
            } else {
                break;
            }
        }
    }

    fn skip_whitespace(&mut self) {
        self.eat_while(|ch| ch.is_ascii_whitespace());
    }

    pub fn next_token(&mut self) -> Result<Token, ParseError> {
        self.skip_whitespace();

        let start = self.offset;

        let Some(ch) = self.peek_char() else {
            return Ok(Token {
                kind: TokenKind::Eof,
                span: Span { offset: start, len: 0 },
            });
        };

        match ch {
            '(' => {
                self.eat_char();
                Ok(Token { kind: TokenKind::LParen, span: Span { offset: start, len: 1 } })
            }
            ')' => {
                self.eat_char();
                Ok(Token { kind: TokenKind::RParen, span: Span { offset: start, len: 1 } })
            }
            ',' => {
                self.eat_char();
                Ok(Token { kind: TokenKind::Comma, span: Span { offset: start, len: 1 } })
            }
            ';' => {
                self.eat_char();
                Ok(Token { kind: TokenKind::Semicolon, span: Span { offset: start, len: 1 } })
            }
            'a'..='z' | 'A'..='Z' | '_' => self.eat_ident_or_keyword(start),
            _ => Err(ParseError {
                message: format!("unexpected character '{ch}'"),
                span: Span { offset: start, len: ch.len_utf8() },
            }),
        }
    }

    fn eat_ident_or_keyword(&mut self, start: usize) -> Result<Token, ParseError> {
        self.eat_while(|ch| ch.is_ascii_alphanumeric() || ch == '_');
        let text = &self.input[start..self.offset];
        let len = self.offset - start;
        let span = Span { offset: start, len };

        let kind = match text.to_ascii_uppercase().as_str() {
            "CREATE" => TokenKind::Create,
            "TABLE" => TokenKind::Table,
            "INDEX" => TokenKind::Index,
            "PRIMARY" => TokenKind::Primary,
            "KEY" => TokenKind::Key,
            "NOT" => TokenKind::Not,
            "NULL" => TokenKind::Null,
            "STRING" => TokenKind::KwString,
            "U32" => TokenKind::KwU32,
            "I32" => TokenKind::KwI32,
            "U64" => TokenKind::KwU64,
            "I64" => TokenKind::KwI64,
            _ => TokenKind::Ident(text.to_string()),
        };

        Ok(Token { kind, span })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lex_all(input: &str) -> Result<Vec<TokenKind>, ParseError> {
        let mut lexer = Lexer::new(input);
        let mut tokens = Vec::new();
        loop {
            let tok = lexer.next_token()?;
            if tok.kind == TokenKind::Eof {
                break;
            }
            tokens.push(tok.kind);
        }
        Ok(tokens)
    }

    #[test]
    fn test_keywords() {
        let tokens = lex_all("CREATE TABLE INDEX PRIMARY KEY NOT NULL").unwrap();
        assert_eq!(tokens, vec![
            TokenKind::Create,
            TokenKind::Table,
            TokenKind::Index,
            TokenKind::Primary,
            TokenKind::Key,
            TokenKind::Not,
            TokenKind::Null,
        ]);
    }

    #[test]
    fn test_type_keywords() {
        let tokens = lex_all("STRING U32 I32 U64 I64").unwrap();
        assert_eq!(tokens, vec![
            TokenKind::KwString,
            TokenKind::KwU32,
            TokenKind::KwI32,
            TokenKind::KwU64,
            TokenKind::KwI64,
        ]);
    }

    #[test]
    fn test_case_insensitive() {
        let tokens = lex_all("create Table string u64").unwrap();
        assert_eq!(tokens, vec![
            TokenKind::Create,
            TokenKind::Table,
            TokenKind::KwString,
            TokenKind::KwU64,
        ]);
    }

    #[test]
    fn test_identifiers() {
        let tokens = lex_all("users my_table col1").unwrap();
        assert_eq!(tokens, vec![
            TokenKind::Ident("users".into()),
            TokenKind::Ident("my_table".into()),
            TokenKind::Ident("col1".into()),
        ]);
    }

    #[test]
    fn test_punctuation() {
        let tokens = lex_all("( ) , ;").unwrap();
        assert_eq!(tokens, vec![
            TokenKind::LParen,
            TokenKind::RParen,
            TokenKind::Comma,
            TokenKind::Semicolon,
        ]);
    }

    #[test]
    fn test_unexpected_character() {
        let err = lex_all("CREATE @").unwrap_err();
        assert_eq!(err.span.offset, 7);
    }

    #[test]
    fn test_full_ddl_tokens() {
        let tokens = lex_all("CREATE TABLE users ( id U64 NOT NULL PRIMARY KEY )").unwrap();
        assert_eq!(tokens, vec![
            TokenKind::Create,
            TokenKind::Table,
            TokenKind::Ident("users".into()),
            TokenKind::LParen,
            TokenKind::Ident("id".into()),
            TokenKind::KwU64,
            TokenKind::Not,
            TokenKind::Null,
            TokenKind::Primary,
            TokenKind::Key,
            TokenKind::RParen,
        ]);
    }
}
