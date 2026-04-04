use crate::planner::plan::PlanFilterPredicate;

use super::eval::eval_predicate;
use super::Columns;

pub fn filter(cols: &Columns, pred: &PlanFilterPredicate) -> Columns {
    let mask = eval_predicate(cols, pred);
    apply_mask(cols, &mask)
}

fn apply_mask(cols: &Columns, mask: &[bool]) -> Columns {
    cols.iter()
        .map(|col| {
            col.iter()
                .zip(mask.iter())
                .filter(|(_, &keep)| keep)
                .map(|(v, _)| v.clone())
                .collect()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execute::scan::scan;
    use crate::storage::CellValue;
    use schema_engine::schema::{ColumnSchema, DataType, TableSchema};

    #[test]
    fn test_filter_reduces_rows() {
        let schema = TableSchema {
            name: "users".into(),
            columns: vec![
                ColumnSchema { name: "id".into(), data_type: DataType::I64, nullable: false },
                ColumnSchema { name: "name".into(), data_type: DataType::String, nullable: false },
                ColumnSchema { name: "age".into(), data_type: DataType::I64, nullable: true },
            ],
            primary_key: vec![0],
            indexes: vec![],
        };
        let mut t = crate::storage::Table::new(schema);
        t.insert(&[CellValue::I64(1), CellValue::Str("Alice".into()), CellValue::I64(30)]).unwrap();
        t.insert(&[CellValue::I64(2), CellValue::Str("Bob".into()), CellValue::I64(25)]).unwrap();
        t.insert(&[CellValue::I64(3), CellValue::Str("Carol".into()), CellValue::I64(35)]).unwrap();

        let cols = scan(&t);
        let filtered = filter(
            &cols,
            &PlanFilterPredicate::GreaterThan {
                column_idx: 2,
                value: query_engine::ast::Value::Int(28),
            },
        );
        assert_eq!(filtered[0].len(), 2);
        assert_eq!(filtered[1], vec![CellValue::Str("Alice".into()), CellValue::Str("Carol".into())]);
    }
}
