//! Wire-format tests for the UUID type.
//!
//! Cross-cutting: covers the `CellValue::Uuid` variant + the standalone
//! `Uuid` newtype, in both serde-JSON and borsh round-trips. Guarded by
//! the respective feature flags so the default `cargo test -p sql-engine`
//! still works without them.

#![cfg(any(feature = "serde", feature = "borsh"))]

use sql_engine::storage::{CellValue, Uuid};

const SAMPLE: &str = "550e8400-e29b-41d4-a716-446655440000";

fn sample_bytes() -> [u8; 16] {
    sql_parser::uuid::parse_uuid(SAMPLE).unwrap()
}

// ── serde / JSON ─────────────────────────────────────────────────────────

#[cfg(feature = "serde")]
#[test]
fn cell_uuid_serializes_as_canonical_string() {
    let cell = CellValue::Uuid(sample_bytes());
    let json = serde_json::to_string(&cell).unwrap();
    assert_eq!(json, format!("\"{SAMPLE}\""));
}

#[cfg(feature = "serde")]
#[test]
fn cell_uuid_round_trip_json() {
    let original = CellValue::Uuid(sample_bytes());
    let json = serde_json::to_string(&original).unwrap();
    // Untagged enum: deserializing a string disambiguates to either Str or
    // Uuid depending on order. We test the explicit Uuid path first.
    let bytes: [u8; 16] = serde_json::from_value::<Uuid>(
        serde_json::from_str(&json).unwrap()
    ).unwrap().0;
    assert_eq!(bytes, sample_bytes());
}

#[cfg(feature = "serde")]
#[test]
fn cell_uuid_invalid_string_fails_deserialize_into_uuid_newtype() {
    let json = "\"not-a-uuid\"";
    let err = serde_json::from_str::<Uuid>(json).unwrap_err();
    assert!(err.to_string().contains("invalid UUID"), "got: {err}");
}

#[cfg(feature = "serde")]
#[test]
fn cell_uuid_nil_round_trip() {
    let cell = CellValue::Uuid([0u8; 16]);
    let json = serde_json::to_string(&cell).unwrap();
    assert_eq!(json, "\"00000000-0000-0000-0000-000000000000\"");
}

#[cfg(feature = "serde")]
#[test]
fn uuid_newtype_round_trip() {
    let u = Uuid(sample_bytes());
    let json = serde_json::to_string(&u).unwrap();
    assert_eq!(json, format!("\"{SAMPLE}\""));
    let back: Uuid = serde_json::from_str(&json).unwrap();
    assert_eq!(back, u);
}

#[cfg(feature = "serde")]
#[test]
fn cell_value_untagged_string_deserializes_to_str_not_uuid() {
    // Pinning: `CellValue` is `#[serde(untagged)]` and `Str` is declared
    // before `Uuid`, so a bare JSON string always deserializes to
    // `CellValue::Str` — even if the contents look like a UUID. Anyone
    // moving `Str` after `Uuid` will silently flip this and break the
    // wire contract; this test fails loudly when that happens.
    let json = format!("\"{SAMPLE}\"");
    let cell: CellValue = serde_json::from_str(&json).unwrap();
    assert_eq!(cell, CellValue::Str(SAMPLE.into()));
    assert!(!matches!(cell, CellValue::Uuid(_)));

    // Non-UUID strings round-trip through Str trivially.
    let cell2: CellValue = serde_json::from_str("\"plain text\"").unwrap();
    assert_eq!(cell2, CellValue::Str("plain text".into()));
}

#[cfg(feature = "serde")]
#[test]
fn cell_value_other_variants_unchanged() {
    // Untagged enum: Uuid must not steal serialization from I64/Str/Null.
    assert_eq!(serde_json::to_string(&CellValue::I64(42)).unwrap(), "42");
    assert_eq!(serde_json::to_string(&CellValue::Str("hi".into())).unwrap(), "\"hi\"");
    assert_eq!(serde_json::to_string(&CellValue::Null).unwrap(), "null");
}

#[cfg(feature = "serde")]
#[test]
fn cell_uuid_in_a_row_serializes_as_array() {
    // A row of mixed cells round-trips as a JSON array.
    let row = vec![
        CellValue::Uuid(sample_bytes()),
        CellValue::Str("Alice".into()),
        CellValue::I64(30),
        CellValue::Null,
    ];
    let json = serde_json::to_string(&row).unwrap();
    assert_eq!(
        json,
        format!("[\"{SAMPLE}\",\"Alice\",30,null]"),
    );
}

// ── borsh ────────────────────────────────────────────────────────────────

#[cfg(feature = "borsh")]
#[test]
fn cell_uuid_borsh_round_trip() {
    use borsh::BorshDeserialize;
    let original = CellValue::Uuid(sample_bytes());
    let bytes = borsh::to_vec(&original).unwrap();
    let back = CellValue::try_from_slice(&bytes).unwrap();
    assert_eq!(original, back);
}

#[cfg(feature = "borsh")]
#[test]
fn uuid_newtype_borsh_round_trip() {
    use borsh::BorshDeserialize;
    let u = Uuid(sample_bytes());
    let bytes = borsh::to_vec(&u).unwrap();
    let back = Uuid::try_from_slice(&bytes).unwrap();
    assert_eq!(u, back);
}

#[cfg(feature = "borsh")]
#[test]
fn cell_uuid_borsh_layout_is_16_bytes_payload() {
    // Sanity: borsh enum tag (1 byte) + 16 bytes of payload.
    let original = CellValue::Uuid([0xab; 16]);
    let bytes = borsh::to_vec(&original).unwrap();
    assert_eq!(bytes.len(), 1 + 16);
}
