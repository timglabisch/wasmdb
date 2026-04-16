use super::token::*;

pub(crate) struct Lexer<'a> {
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

    fn eat_while(&mut self, pred: impl Fn(char) -> bool) {
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
                span: Span {
                    offset: start,
                    len: 0,
                },
            });
        };

        match ch {
            'a'..='z' | 'A'..='Z' | '_' => self.eat_ident_or_keyword(start),
            '0'..='9' => self.eat_number(start),
            '\'' => self.eat_string(start),
            '=' => {
                self.eat_char();
                Ok(self.token(TokenKind::Eq, start))
            }
            '!' => {
                self.eat_char();
                if self.peek_char() == Some('=') {
                    self.eat_char();
                    Ok(self.token(TokenKind::Neq, start))
                } else {
                    Err(ParseError::new(
                        "expected '=' after '!'",
                        Span {
                            offset: start,
                            len: 1,
                        },
                    ))
                }
            }
            '<' => {
                self.eat_char();
                if self.peek_char() == Some('=') {
                    self.eat_char();
                    Ok(self.token(TokenKind::Lte, start))
                } else {
                    Ok(self.token(TokenKind::Lt, start))
                }
            }
            '>' => {
                self.eat_char();
                if self.peek_char() == Some('=') {
                    self.eat_char();
                    Ok(self.token(TokenKind::Gte, start))
                } else {
                    Ok(self.token(TokenKind::Gt, start))
                }
            }
            ',' => {
                self.eat_char();
                Ok(self.token(TokenKind::Comma, start))
            }
            '.' => {
                self.eat_char();
                Ok(self.token(TokenKind::Dot, start))
            }
            '(' => {
                self.eat_char();
                Ok(self.token(TokenKind::LParen, start))
            }
            ')' => {
                self.eat_char();
                Ok(self.token(TokenKind::RParen, start))
            }
            '*' => {
                self.eat_char();
                Ok(self.token(TokenKind::Star, start))
            }
            ';' => {
                self.eat_char();
                Ok(self.token(TokenKind::Semicolon, start))
            }
            ':' => {
                self.eat_char();
                let name_start = self.offset;
                self.eat_while(|ch| ch.is_ascii_alphanumeric() || ch == '_');
                if self.offset == name_start {
                    return Err(ParseError::new(
                        "expected identifier after ':'",
                        Span { offset: start, len: 1 },
                    ));
                }
                let name = self.input[name_start..self.offset].to_string();
                Ok(self.token(TokenKind::Placeholder(name), start))
            }
            other => Err(ParseError::new(
                format!("unexpected character '{}'", other),
                Span {
                    offset: start,
                    len: other.len_utf8(),
                },
            )),
        }
    }

    fn token(&self, kind: TokenKind, start: usize) -> Token {
        Token {
            kind,
            span: Span {
                offset: start,
                len: self.offset - start,
            },
        }
    }

    fn eat_ident_or_keyword(&mut self, start: usize) -> Result<Token, ParseError> {
        self.eat_while(|ch| ch.is_ascii_alphanumeric() || ch == '_');
        let text = &self.input[start..self.offset];
        let kind = match text.to_ascii_uppercase().as_str() {
            "SELECT" => TokenKind::Select,
            "FROM" => TokenKind::From,
            "WHERE" => TokenKind::Where,
            "JOIN" => TokenKind::Join,
            "INNER" => TokenKind::Inner,
            "LEFT" => TokenKind::Left,
            "ON" => TokenKind::On,
            "GROUP" => TokenKind::Group,
            "BY" => TokenKind::By,
            "AND" => TokenKind::And,
            "OR" => TokenKind::Or,
            "IS" => TokenKind::Is,
            "NOT" => TokenKind::Not,
            "NULL" => TokenKind::Null,
            "AS" => TokenKind::As,
            "TRUE" => TokenKind::True,
            "FALSE" => TokenKind::False,
            "COUNT" => TokenKind::Count,
            "SUM" => TokenKind::Sum,
            "MIN" => TokenKind::Min,
            "MAX" => TokenKind::Max,
            "ORDER" => TokenKind::Order,
            "ASC" => TokenKind::Asc,
            "DESC" => TokenKind::Desc,
            "IN" => TokenKind::In,
            "LIMIT" => TokenKind::Limit,
            "INSERT" => TokenKind::Insert,
            "INTO" => TokenKind::Into,
            "VALUES" => TokenKind::Values,
            "DELETE" => TokenKind::Delete,
            "UPDATE" => TokenKind::Update,
            "SET" => TokenKind::Set,
            "REACTIVE" => TokenKind::Reactive,
            "CREATE" => TokenKind::Create,
            _ => TokenKind::Ident(text.to_string()),
        };
        Ok(self.token(kind, start))
    }

    fn eat_number(&mut self, start: usize) -> Result<Token, ParseError> {
        self.eat_while(|ch| ch.is_ascii_digit());
        if self.peek_char() == Some('.') {
            // Check it's a dot followed by a digit (not table.column after a number)
            let after_dot = self.input[self.offset + 1..].chars().next();
            if after_dot.map_or(false, |ch| ch.is_ascii_digit()) {
                self.eat_char(); // consume '.'
                self.eat_while(|ch| ch.is_ascii_digit());
                let text = &self.input[start..self.offset];
                let value: f64 = text.parse().map_err(|_| {
                    ParseError::new(
                        format!("invalid float '{}'", text),
                        Span {
                            offset: start,
                            len: self.offset - start,
                        },
                    )
                })?;
                return Ok(self.token(TokenKind::Float(value), start));
            }
        }
        let text = &self.input[start..self.offset];
        let value: i64 = text.parse().map_err(|_| {
            ParseError::new(
                format!("invalid integer '{}'", text),
                Span {
                    offset: start,
                    len: self.offset - start,
                },
            )
        })?;
        Ok(self.token(TokenKind::Integer(value), start))
    }

    fn eat_string(&mut self, start: usize) -> Result<Token, ParseError> {
        self.eat_char(); // consume opening quote
        let content_start = self.offset;
        loop {
            match self.peek_char() {
                Some('\'') => {
                    let content = self.input[content_start..self.offset].to_string();
                    self.eat_char(); // consume closing quote
                    return Ok(self.token(TokenKind::Str(content), start));
                }
                Some(_) => {
                    self.eat_char();
                }
                None => {
                    return Err(ParseError::new(
                        "unterminated string literal",
                        Span {
                            offset: start,
                            len: self.offset - start,
                        },
                    ));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lex_all(input: &str) -> Result<Vec<Token>, ParseError> {
        let mut lexer = Lexer::new(input);
        let mut tokens = vec![];
        loop {
            let tok = lexer.next_token()?;
            if tok.kind == TokenKind::Eof {
                tokens.push(tok);
                break;
            }
            tokens.push(tok);
        }
        Ok(tokens)
    }

    #[test]
    fn test_lex_keywords() {
        let tokens = lex_all("SELECT FROM WHERE").unwrap();
        assert_eq!(tokens[0].kind, TokenKind::Select);
        assert_eq!(tokens[1].kind, TokenKind::From);
        assert_eq!(tokens[2].kind, TokenKind::Where);
    }

    #[test]
    fn test_lex_case_insensitive() {
        let tokens = lex_all("select FROM where").unwrap();
        assert_eq!(tokens[0].kind, TokenKind::Select);
        assert_eq!(tokens[1].kind, TokenKind::From);
        assert_eq!(tokens[2].kind, TokenKind::Where);
    }

    #[test]
    fn test_lex_identifiers() {
        let tokens = lex_all("users foo_bar _x").unwrap();
        assert_eq!(tokens[0].kind, TokenKind::Ident("users".into()));
        assert_eq!(tokens[1].kind, TokenKind::Ident("foo_bar".into()));
        assert_eq!(tokens[2].kind, TokenKind::Ident("_x".into()));
    }

    #[test]
    fn test_lex_numbers() {
        let tokens = lex_all("42 3.14").unwrap();
        assert_eq!(tokens[0].kind, TokenKind::Integer(42));
        assert_eq!(tokens[1].kind, TokenKind::Float(3.14));
    }

    #[test]
    fn test_lex_string() {
        let tokens = lex_all("'hello world'").unwrap();
        assert_eq!(tokens[0].kind, TokenKind::Str("hello world".into()));
    }

    #[test]
    fn test_lex_unterminated_string() {
        let err = lex_all("'hello").unwrap_err();
        assert!(err.message.contains("unterminated"));
    }

    #[test]
    fn test_lex_operators() {
        let tokens = lex_all("= != < > <= >=").unwrap();
        assert_eq!(tokens[0].kind, TokenKind::Eq);
        assert_eq!(tokens[1].kind, TokenKind::Neq);
        assert_eq!(tokens[2].kind, TokenKind::Lt);
        assert_eq!(tokens[3].kind, TokenKind::Gt);
        assert_eq!(tokens[4].kind, TokenKind::Lte);
        assert_eq!(tokens[5].kind, TokenKind::Gte);
    }

    #[test]
    fn test_lex_punctuation() {
        let tokens = lex_all(", . ( ) *").unwrap();
        assert_eq!(tokens[0].kind, TokenKind::Comma);
        assert_eq!(tokens[1].kind, TokenKind::Dot);
        assert_eq!(tokens[2].kind, TokenKind::LParen);
        assert_eq!(tokens[3].kind, TokenKind::RParen);
        assert_eq!(tokens[4].kind, TokenKind::Star);
    }

    #[test]
    fn test_lex_spans() {
        let tokens = lex_all("SELECT users").unwrap();
        assert_eq!(tokens[0].span, Span { offset: 0, len: 6 });
        assert_eq!(tokens[1].span, Span { offset: 7, len: 5 });
    }

    #[test]
    fn test_lex_unexpected_char() {
        let err = lex_all("SELECT @").unwrap_err();
        assert!(err.message.contains("unexpected character"));
    }

    #[test]
    fn test_lex_placeholder() {
        let tokens = lex_all(":foo_bar").unwrap();
        assert_eq!(tokens[0].kind, TokenKind::Placeholder("foo_bar".into()));
    }

    #[test]
    fn test_lex_reactive() {
        let tokens = lex_all("REACTIVE").unwrap();
        assert_eq!(tokens[0].kind, TokenKind::Reactive);

        let tokens = lex_all("reactive").unwrap();
        assert_eq!(tokens[0].kind, TokenKind::Reactive);
    }

    #[test]
    fn test_lex_colon_without_name_error() {
        let err = lex_all(": ").unwrap_err();
        assert!(err.message.contains("expected identifier after ':'"));
    }
}
