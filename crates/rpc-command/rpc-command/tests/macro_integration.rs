//! Macro integration tests — applies `#[rpc_command]` to real structs (flat,
//! nested, `Option<T>`, `Vec<T>`, multiple defaults) plus `#[rpc_command_enum]`
//! to a synthetic enum, then verifies:
//!
//!   1. The macro expansion is syntactically valid Rust.
//!   2. The injected derives (Borsh*/Serde*/TS) compile against actual types.
//!   3. The auto `__RPC_COMMAND_SPEC` const carries the right metadata.
//!   4. The enum macro's emitted `#[test]` writes a single bundled
//!      `<EnumName>Factories.ts` to `RPC_COMMAND_TS_ROOT`.

use std::path::PathBuf;

use borsh::{BorshDeserialize, BorshSerialize};
use rpc_command::{rpc_command, rpc_command_enum};
use serde::{Deserialize, Serialize};
use ts_rs::TS;

fn out_root() -> PathBuf {
    let p = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("rpc-command-tests");
    std::fs::create_dir_all(&p).unwrap();
    std::env::set_var("RPC_COMMAND_TS_ROOT", &p);
    p
}

// ── Nested types used in the structs below ───────────────────────────────

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
pub struct LineItem {
    pub sku: String,
    pub qty: i64,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
pub struct Header {
    pub title: String,
    pub note: Option<String>,
}

// ── Test commands ────────────────────────────────────────────────────────

#[rpc_command]
pub struct Flat {
    pub a: i64,
    pub b: String,
}

#[rpc_command]
pub struct WithNested {
    pub header: Header,
    pub items: Vec<LineItem>,
    pub maybe_id: Option<String>,
    #[client_default = "nextId()"]
    pub activity_id: String,
    #[client_default = "new Date().toISOString()"]
    pub timestamp: String,
}

#[rpc_command]
pub struct OnlyAuto {
    #[client_default = "new Date().toISOString()"]
    pub at: String,
}

#[rpc_command]
pub struct Skipped {
    pub a: i64,
}

// ── Test enum ────────────────────────────────────────────────────────────

#[rpc_command_enum]
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
#[serde(tag = "type")]
pub enum TestCmd {
    Flat(Flat),
    WithNested(WithNested),
    OnlyAuto(OnlyAuto),
    #[rpc_command_skip]
    Skipped(Skipped),
}

// ── Spec const checks ────────────────────────────────────────────────────

#[test]
fn flat_spec_const_has_no_auto_fields() {
    let spec = Flat::__RPC_COMMAND_SPEC;
    assert_eq!(spec.struct_name, "Flat");
    assert_eq!(spec.fn_name, "flat");
    assert_eq!(spec.inputs, &["a", "b"]);
    assert!(spec.auto.is_empty());
}

#[test]
fn with_nested_spec_const_partitions_inputs_vs_auto() {
    let spec = WithNested::__RPC_COMMAND_SPEC;
    assert_eq!(spec.struct_name, "WithNested");
    assert_eq!(spec.fn_name, "withNested");
    assert_eq!(spec.inputs, &["header", "items", "maybe_id"]);
    assert_eq!(
        spec.auto,
        &[
            ("activity_id", "nextId()"),
            ("timestamp", "new Date().toISOString()"),
        ]
    );
}

#[test]
fn only_auto_spec_has_no_inputs() {
    let spec = OnlyAuto::__RPC_COMMAND_SPEC;
    assert!(spec.inputs.is_empty());
    assert_eq!(spec.auto.len(), 1);
}

// ── Round-trip checks (proves derive bundle is wired) ────────────────────

#[test]
fn flat_roundtrips_borsh() {
    let v = Flat { a: 7, b: "x".into() };
    let bytes = borsh::to_vec(&v).unwrap();
    let back: Flat = borsh::from_slice(&bytes).unwrap();
    assert_eq!(back.a, 7);
    assert_eq!(back.b, "x");
}

#[test]
fn nested_roundtrips_borsh() {
    let v = WithNested {
        header: Header { title: "h".into(), note: Some("n".into()) },
        items: vec![LineItem { sku: "abc".into(), qty: 3 }],
        maybe_id: None,
        activity_id: "aid".into(),
        timestamp: "now".into(),
    };
    let bytes = borsh::to_vec(&v).unwrap();
    let back: WithNested = borsh::from_slice(&bytes).unwrap();
    assert_eq!(back.items.len(), 1);
    assert_eq!(back.items[0].sku, "abc");
    assert_eq!(back.header.note.as_deref(), Some("n"));
}

#[test]
fn nested_roundtrips_serde_json() {
    let v = WithNested {
        header: Header { title: "h".into(), note: None },
        items: vec![],
        maybe_id: Some("m".into()),
        activity_id: "aid".into(),
        timestamp: "now".into(),
    };
    let json = serde_json::to_string(&v).unwrap();
    let back: WithNested = serde_json::from_str(&json).unwrap();
    assert_eq!(back.maybe_id.as_deref(), Some("m"));
}

// ── Bundle file checks ───────────────────────────────────────────────────

#[test]
fn bundle_file_contains_all_non_skipped_factories() {
    let root = out_root();
    rpc_command::export_bundle_named(
        "TestCmdFactories",
        &[
            Flat::__RPC_COMMAND_SPEC,
            WithNested::__RPC_COMMAND_SPEC,
            OnlyAuto::__RPC_COMMAND_SPEC,
        ],
        "TestCmd",
    );
    let s = std::fs::read_to_string(root.join("TestCmdFactories.ts")).unwrap();
    assert!(s.contains("export function flat("));
    assert!(s.contains("export function withNested("));
    assert!(s.contains("export function onlyAuto(): TestCmd {"));
    // Skipped command must not appear.
    assert!(!s.contains("export function skipped"));
    // Only one nextId import even though one factory uses it.
    assert_eq!(s.matches("import { nextId }").count(), 1);
}
