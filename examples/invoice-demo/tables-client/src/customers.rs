//! `Customers` — client-side marker + wire types.

use borsh::{BorshSerialize, BorshDeserialize};
use serde::{Serialize, Deserialize};
use tables::{Params, Row, Table, TableId};

pub struct Customers;

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize)]
pub struct CustomersParams {
    pub owner_id: i64,
}
impl Params for CustomersParams {}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize)]
pub struct CustomersRow {
    pub id: i64,
    pub name: String,
}
impl Row for CustomersRow {
    type Pk = i64;
    fn pk(&self) -> i64 { self.id }
}

impl Table for Customers {
    const ID: TableId = "invoice_demo::Customers";
    type Params = CustomersParams;
    type Row = CustomersRow;
}
