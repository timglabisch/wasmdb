use borsh::{BorshSerialize, BorshDeserialize};
use serde::{Serialize, Deserialize};
use ts_rs::TS;
use database::Database;
use sql_engine::execute::Params;
use sync::command::CommandError;
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int, p_str};

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
pub struct CreatePayment {
    pub id: i64,
    pub invoice_id: i64,
    pub amount: i64,
    pub paid_at: String,
    pub method: String,
    pub reference: String,
    pub note: String,
}

impl CreatePayment {
    pub fn execute(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        let params = Params::from([
            p_int("id", self.id),
            p_int("invoice_id", self.invoice_id),
            p_int("amount", self.amount),
            p_str("paid_at", &self.paid_at),
            p_str("method", &self.method),
            p_str("reference", &self.reference),
            p_str("note", &self.note),
        ]);
        execute_sql(db,
            "INSERT INTO payments (id, invoice_id, amount, paid_at, method, reference, note) \
             VALUES (:id, :invoice_id, :amount, :paid_at, :method, :reference, :note)",
            params)
    }
}
