use core::fmt;

// ── Span & Error ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub offset: usize,
    pub len: usize,
}

#[derive(Debug, Clone)]
pub struct ParseError {
    pub message: String,
    pub span: Span,
}

impl ParseError {
    pub(crate) fn new(message: impl Into<String>, span: Span) -> Self {
        Self {
            message: message.into(),
            span,
        }
    }

    /// Render a human-readable error with source context.
    pub fn render(&self, input: &str) -> String {
        let mut line_start = 0;
        let mut line_no = 1;
        for (i, ch) in input.char_indices() {
            if i >= self.span.offset {
                break;
            }
            if ch == '\n' {
                line_start = i + 1;
                line_no += 1;
            }
        }
        let line_end = input[line_start..]
            .find('\n')
            .map(|p| line_start + p)
            .unwrap_or(input.len());
        let line_text = &input[line_start..line_end];
        let col = self.span.offset - line_start;

        let mut out = String::new();
        out.push_str(&format!(
            "parse error (line {}): {}\n",
            line_no, self.message
        ));
        out.push_str(&format!("  {}\n", line_text));
        out.push_str(&format!("  {}^", " ".repeat(col)));
        out
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "parse error: {}", self.message)
    }
}

impl std::error::Error for ParseError {}

// ── Token ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub kind: TokenKind,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind {
    // Keywords
    Select,
    From,
    Where,
    Join,
    Inner,
    Left,
    On,
    Group,
    By,
    And,
    Or,
    Is,
    Not,
    Null,
    As,
    True,
    False,
    // Aggregate keywords
    Count,
    Sum,
    Min,
    Max,
    // ORDER BY
    Order,
    Asc,
    Desc,
    // LIMIT
    Limit,
    // Literals
    Integer(i64),
    Float(f64),
    Str(String),
    // Identifier
    Ident(String),
    // Operators
    Eq,
    Neq,
    Lt,
    Gt,
    Lte,
    Gte,
    // Punctuation
    Comma,
    Dot,
    LParen,
    RParen,
    Star,
    // End
    Eof,
}

impl TokenKind {
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Self::Select => "SELECT",
            Self::From => "FROM",
            Self::Where => "WHERE",
            Self::Join => "JOIN",
            Self::Inner => "INNER",
            Self::Left => "LEFT",
            Self::On => "ON",
            Self::Group => "GROUP",
            Self::By => "BY",
            Self::And => "AND",
            Self::Or => "OR",
            Self::Is => "IS",
            Self::Not => "NOT",
            Self::Null => "NULL",
            Self::As => "AS",
            Self::True => "TRUE",
            Self::False => "FALSE",
            Self::Count => "COUNT",
            Self::Sum => "SUM",
            Self::Min => "MIN",
            Self::Max => "MAX",
            Self::Order => "ORDER",
            Self::Asc => "ASC",
            Self::Desc => "DESC",
            Self::Limit => "LIMIT",
            Self::Integer(_) => "integer",
            Self::Float(_) => "float",
            Self::Str(_) => "string",
            Self::Ident(_) => "identifier",
            Self::Eq => "'='",
            Self::Neq => "'!='",
            Self::Lt => "'<'",
            Self::Gt => "'>'",
            Self::Lte => "'<='",
            Self::Gte => "'>='",
            Self::Comma => "','",
            Self::Dot => "'.'",
            Self::LParen => "'('",
            Self::RParen => "')'",
            Self::Star => "'*'",
            Self::Eof => "end of input",
        }
    }

    /// Match only on variant, ignoring inner values.
    pub(crate) fn matches(&self, other: &TokenKind) -> bool {
        core::mem::discriminant(self) == core::mem::discriminant(other)
    }
}
