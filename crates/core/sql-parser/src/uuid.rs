//! UUID parsing/formatting helpers for the canonical hyphenated form
//! (`8-4-4-4-12` lowercase hex). Backing storage is a raw `[u8; 16]` so
//! every layer down to the column store can pass the bytes through
//! without re-validating.

/// Parse the canonical hyphenated form into 16 raw bytes.
/// Accepts upper- and lower-case hex; rejects everything else (no
/// braces, no URN prefix, no hex-only short form).
pub fn parse_uuid(s: &str) -> Option<[u8; 16]> {
    let bytes = s.as_bytes();
    if bytes.len() != 36 {
        return None;
    }
    if bytes[8] != b'-' || bytes[13] != b'-' || bytes[18] != b'-' || bytes[23] != b'-' {
        return None;
    }
    let mut out = [0u8; 16];
    let mut byte_idx = 0;
    let mut i = 0;
    while i < 36 {
        if i == 8 || i == 13 || i == 18 || i == 23 {
            i += 1;
            continue;
        }
        let hi = hex_nibble(bytes[i])?;
        let lo = hex_nibble(bytes[i + 1])?;
        out[byte_idx] = (hi << 4) | lo;
        byte_idx += 1;
        i += 2;
    }
    Some(out)
}

/// Format 16 raw bytes as the canonical hyphenated lowercase form.
pub fn format_uuid(bytes: &[u8; 16]) -> String {
    let mut s = String::with_capacity(36);
    for (i, b) in bytes.iter().enumerate() {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0xf) as usize] as char);
        if matches!(i, 3 | 5 | 7 | 9) {
            s.push('-');
        }
    }
    s
}

fn hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

const HEX: &[u8; 16] = b"0123456789abcdef";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_round_trip_lowercase() {
        let s = "550e8400-e29b-41d4-a716-446655440000";
        let bytes = parse_uuid(s).unwrap();
        assert_eq!(format_uuid(&bytes), s);
    }

    #[test]
    fn parse_uppercase_is_accepted_but_format_is_lower() {
        let s = "550E8400-E29B-41D4-A716-446655440000";
        let bytes = parse_uuid(s).unwrap();
        assert_eq!(format_uuid(&bytes), s.to_ascii_lowercase());
    }

    #[test]
    fn parse_rejects_wrong_length() {
        assert!(parse_uuid("550e8400-e29b-41d4-a716-44665544000").is_none());
        assert!(parse_uuid("550e8400-e29b-41d4-a716-4466554400000").is_none());
    }

    #[test]
    fn parse_rejects_misplaced_hyphens() {
        assert!(parse_uuid("550e84000e29b-41d4-a716-446655440000").is_none());
    }

    #[test]
    fn parse_rejects_non_hex() {
        assert!(parse_uuid("550e8400-e29b-41d4-a716-44665544000g").is_none());
    }

    #[test]
    fn nil_uuid() {
        let s = "00000000-0000-0000-0000-000000000000";
        assert_eq!(parse_uuid(s), Some([0u8; 16]));
        assert_eq!(format_uuid(&[0u8; 16]), s);
    }

    #[test]
    fn max_uuid() {
        let s = "ffffffff-ffff-ffff-ffff-ffffffffffff";
        assert_eq!(parse_uuid(s), Some([0xffu8; 16]));
        assert_eq!(format_uuid(&[0xffu8; 16]), s);
    }

    #[test]
    fn parse_mixed_case() {
        let s = "550E8400-e29b-41D4-a716-446655440000";
        let bytes = parse_uuid(s).unwrap();
        assert_eq!(format_uuid(&bytes), "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn parse_rejects_empty_string() {
        assert!(parse_uuid("").is_none());
    }

    #[test]
    fn parse_rejects_internal_whitespace() {
        assert!(parse_uuid("550e8400 e29b-41d4-a716-446655440000").is_none());
        assert!(parse_uuid("550e8400-e29b-41d4-a716-44665544 0000").is_none());
    }

    #[test]
    fn parse_rejects_leading_or_trailing_whitespace() {
        assert!(parse_uuid(" 550e8400-e29b-41d4-a716-446655440000").is_none());
        assert!(parse_uuid("550e8400-e29b-41d4-a716-446655440000 ").is_none());
    }

    #[test]
    fn parse_rejects_non_ascii() {
        // Replace one nibble with `ä` (2 UTF-8 bytes) — string len becomes 37.
        assert!(parse_uuid("550e840ä-e29b-41d4-a716-446655440000").is_none());
    }

    #[test]
    fn parse_rejects_braces_form() {
        assert!(parse_uuid("{550e8400-e29b-41d4-a716-446655440000}").is_none());
    }

    #[test]
    fn parse_rejects_urn_form() {
        assert!(parse_uuid("urn:uuid:550e8400-e29b-41d4-a716-446655440000").is_none());
    }

    #[test]
    fn parse_rejects_hex_only_form() {
        assert!(parse_uuid("550e8400e29b41d4a716446655440000").is_none());
    }

    #[test]
    fn property_round_trip_random_bytes() {
        // Deterministic LCG — no extra deps; covers the byte space densely enough
        // to catch off-by-one nibble bugs.
        let mut state: u64 = 0xdead_beef_cafe_babe;
        for _ in 0..1024 {
            let mut bytes = [0u8; 16];
            for slot in &mut bytes {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                *slot = (state >> 33) as u8;
            }
            let s = format_uuid(&bytes);
            assert_eq!(s.len(), 36);
            assert_eq!(parse_uuid(&s), Some(bytes), "round-trip failed for {s}");
        }
    }
}
