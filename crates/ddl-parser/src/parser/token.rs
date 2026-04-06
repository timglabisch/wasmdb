#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub offset: usize,
    pub len: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub kind: TokenKind,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind {
    // Keywords
    Create,
    Table,
    Index,
    Primary,
    Key,
    Not,
    Null,
    Using,
    BTree,
    Hash,
    // Type keywords
    KwI64,
    KwString,
    // Identifier
    Ident(String),
    // Punctuation
    LParen,
    RParen,
    Comma,
    Semicolon,
    // End
    Eof,
}

#[derive(Debug)]
pub struct ParseError {
    pub message: String,
    pub span: Span,
}

impl ParseError {
    pub fn render(&self, input: &str) -> String {
        let mut line_start = 0;
        for (i, ch) in input.char_indices() {
            if i >= self.span.offset {
                break;
            }
            if ch == '\n' {
                line_start = i + 1;
            }
        }
        let line_end = input[self.span.offset..]
            .find('\n')
            .map(|p| self.span.offset + p)
            .unwrap_or(input.len());
        let line = &input[line_start..line_end];
        let col = self.span.offset - line_start;
        format!(
            "parse error: {}\n  {}\n  {}^",
            self.message,
            line,
            " ".repeat(col),
        )
    }
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ParseError {}
