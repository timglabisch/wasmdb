//! Walk a SQL string and collect the names of `{identifier}` placeholders.
//! Mirrors the runtime renderer: ignores `'…'` strings and `--` / `/* */`
//! comments, treats `{{` and `}}` as escaped literal braces. Used at compile
//! time to validate that every placeholder has a binding (and every binding
//! is referenced).

use proc_macro2::Span;

#[derive(Debug)]
pub struct ParseError {
    pub message: String,
    pub _span: Span,
}

pub fn collect_placeholders(sql: &str, span: Span) -> Result<Vec<String>, ParseError> {
    let mut out = Vec::new();
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\'' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if b == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            i += 2;
            while i < bytes.len() {
                if bytes[i] == b'*' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
                    i += 2;
                    break;
                }
                i += 1;
            }
            continue;
        }
        if b == b'{' {
            if i + 1 < bytes.len() && bytes[i + 1] == b'{' {
                i += 2;
                continue;
            }
            let start = i + 1;
            if start >= bytes.len() || !is_ident_start(bytes[start]) {
                return Err(ParseError {
                    message: format!("expected identifier after `{{` at byte {i}; use `{{{{` for a literal brace"),
                    _span: span,
                });
            }
            let mut end = start;
            while end < bytes.len() && is_ident_cont(bytes[end]) {
                end += 1;
            }
            if end >= bytes.len() || bytes[end] != b'}' {
                return Err(ParseError {
                    message: format!("missing `}}` to close placeholder starting at byte {i}"),
                    _span: span,
                });
            }
            out.push(sql[start..end].to_string());
            i = end + 1;
            continue;
        }
        if b == b'}' {
            if i + 1 < bytes.len() && bytes[i + 1] == b'}' {
                i += 2;
                continue;
            }
            return Err(ParseError {
                message: format!("unexpected `}}` at byte {i}; use `}}}}` for a literal brace"),
                _span: span,
            });
        }
        i += 1;
    }
    Ok(out)
}

fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}

fn is_ident_cont(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}
