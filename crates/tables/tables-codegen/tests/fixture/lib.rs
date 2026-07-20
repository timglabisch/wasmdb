//! Scan fixture for tables-codegen unit tests. Never compiled — only
//! parsed by `parse::scan`, so the item bodies just need valid syntax.

#[row]
pub struct DraftEvent {
    #[pk]
    pub command_id: i64,
    pub doc_id: i64,
    pub seq: i64,
    pub val: i64,
}

#[row]
pub struct DraftTotal {
    #[pk]
    pub doc_id: i64,
    pub amount: i64,
}

#[projection_row]
pub struct DraftLog {
    pub command_id: Uuid,
    pub doc_id: i64,
}

#[derive(Default, Clone)]
pub struct DraftFoldView {
    pub doc_id: i64,
    pub amount: i64,
}

#[projection(outputs(DraftTotal))]
impl DraftFoldView {
    fn apply(&mut self, row: &DraftLog) -> Result<(), String> {
        self.amount += 1;
        Ok(())
    }

    fn render(&self, ctx: &RenderCtx<'_>, out: &mut Out) -> Result<(), String> {
        out.emit(DraftTotal { doc_id: self.doc_id, amount: self.amount });
        Ok(())
    }
}
