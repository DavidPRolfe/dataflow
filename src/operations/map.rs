use super::data::{DataType, RowUpdate, Source, Updates};
use super::Operation;

/// Map will alter all incoming rows to match the sources. This may reorder columns, add new
/// copies of columns, or add new columns of literals
pub struct Map {
    pub sources: Vec<Source>,
}

impl Operation for Map {
    fn process(&mut self, updates: Updates) -> Vec<RowUpdate> {
        updates
            .updates
            .iter()
            .map(|update| {
                let mapped_row = self
                    .sources
                    .iter()
                    .map(|source| match source {
                        Source::Column(c) => update[*c].clone(),
                        Source::Literal(d) => d.clone(),
                    })
                    .collect::<Vec<DataType>>()
                    .into();

                match update {
                    RowUpdate::Add(_) => RowUpdate::Add(mapped_row),
                    RowUpdate::Remove(_) => RowUpdate::Remove(mapped_row),
                }
            })
            .collect()
    }
}
