use crate::nodes::data::{DataType, RowUpdate, RowUpdates, Source};
use crate::nodes::Updater;

/// Map node will alter all incoming rows to match the sources. This may reorder columns, add new
/// copies of columns, or add new columns of literals
struct Map {
    sources: Vec<Source>,
}

impl Updater for Map {
    fn process(&mut self, updates: RowUpdates) -> RowUpdates {
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
            .collect::<Vec<RowUpdate>>()
            .into()
    }
}
