//! `FromRow` / `FromCell` traits for typed reads via [`SqlStmt::read_row`]
//! and [`SqlStmt::read_rows`].
//!
//! Mapping is **positional**: the engine returns columns in the order they
//! appear in the `SELECT`, and tuples / `#[derive(FromRow)]` structs are
//! filled in their declared order. The macro does not look at column names.

use sql_engine::storage::{CellValue, Uuid};
use sync::command::CommandError;

/// Convert a single [`CellValue`] into a typed value. `Null` cells map to
/// `None` for `Option<T>`; for non-optional targets, `Null` is an error.
pub trait FromCell: Sized {
    fn from_cell(v: CellValue) -> Result<Self, CommandError>;
}

impl FromCell for i64 {
    fn from_cell(v: CellValue) -> Result<Self, CommandError> {
        match v {
            CellValue::I64(n) => Ok(n),
            other => Err(CommandError::ExecutionFailed(format!(
                "FromCell<i64>: expected I64, got {other:?}"
            ))),
        }
    }
}

impl FromCell for String {
    fn from_cell(v: CellValue) -> Result<Self, CommandError> {
        match v {
            CellValue::Str(s) => Ok(s),
            other => Err(CommandError::ExecutionFailed(format!(
                "FromCell<String>: expected Str, got {other:?}"
            ))),
        }
    }
}

impl FromCell for Uuid {
    fn from_cell(v: CellValue) -> Result<Self, CommandError> {
        match v {
            CellValue::Uuid(b) => Ok(Uuid(b)),
            other => Err(CommandError::ExecutionFailed(format!(
                "FromCell<Uuid>: expected Uuid, got {other:?}"
            ))),
        }
    }
}

impl<T: FromCell> FromCell for Option<T> {
    fn from_cell(v: CellValue) -> Result<Self, CommandError> {
        match v {
            CellValue::Null => Ok(None),
            other => T::from_cell(other).map(Some),
        }
    }
}

/// A row's worth of columns, mapped positionally to a Rust type.
/// Implemented for tuples up to 16 elements and via `#[derive(FromRow)]`
/// for named-field structs.
pub trait FromRow: Sized {
    const COLS: usize;
    fn from_row(row: Vec<CellValue>) -> Result<Self, CommandError>;
}

macro_rules! impl_from_row_tuple {
    ($n:expr; $($T:ident),+) => {
        impl<$($T: FromCell),+> FromRow for ($($T,)+) {
            const COLS: usize = $n;
            fn from_row(row: Vec<CellValue>) -> Result<Self, CommandError> {
                if row.len() != $n {
                    return Err(CommandError::ExecutionFailed(format!(
                        "FromRow tuple<{}>: expected {} columns, got {}",
                        $n, $n, row.len()
                    )));
                }
                let mut it = row.into_iter();
                Ok(( $( <$T as FromCell>::from_cell(it.next().unwrap())?, )+ ))
            }
        }
    };
}

impl_from_row_tuple!(1;  T1);
impl_from_row_tuple!(2;  T1, T2);
impl_from_row_tuple!(3;  T1, T2, T3);
impl_from_row_tuple!(4;  T1, T2, T3, T4);
impl_from_row_tuple!(5;  T1, T2, T3, T4, T5);
impl_from_row_tuple!(6;  T1, T2, T3, T4, T5, T6);
impl_from_row_tuple!(7;  T1, T2, T3, T4, T5, T6, T7);
impl_from_row_tuple!(8;  T1, T2, T3, T4, T5, T6, T7, T8);
impl_from_row_tuple!(9;  T1, T2, T3, T4, T5, T6, T7, T8, T9);
impl_from_row_tuple!(10; T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
impl_from_row_tuple!(11; T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
impl_from_row_tuple!(12; T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);
impl_from_row_tuple!(13; T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13);
impl_from_row_tuple!(14; T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14);
impl_from_row_tuple!(15; T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15);
impl_from_row_tuple!(16; T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16);
