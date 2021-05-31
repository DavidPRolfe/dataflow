use crate::nodes::data::{DataType, RowUpdate, RowUpdates, Source};
use crate::nodes::Updater;
use crate::nodes::state::State;

/// Count is used to get the non-distinct count of rows with non null values passing through it.
/// It can be optionally be grouped by any number of columns.
///
/// TODO: Make this work for distinct values. Should only need to add another field to the stored group
/// during processing?
struct Count<S: State> {
    source: Source,
    group: Vec<usize>,
    state: S,
}

impl<S: State> Count<S> {
    fn get_count(&self, group: &Vec<DataType>) -> Option<i32> {
        let data = self.state.get(group);

        match data.len() {
            0 => None,
            1 => match data[0] {
                DataType::Integer(c) => Some(c),
                _ => unreachable!("Count will only ever store ints"),
            },
            _ => unreachable!("Count state will only ever hold one value"),
        }
    }

    fn set_count(&mut self, group: Vec<DataType>, value: i32) {
        self.state.set(group, vec![DataType::Integer(value)])
    }
}

impl<S: State> Updater for Count<S> {
    fn process(&mut self, mut updates: RowUpdates) -> RowUpdates {
        for mut update in updates.updates.iter_mut() {
            let mut group = vec![];
            for column in &self.group {
                group.push(update[*column].clone())
            }

            let cur = self.get_count(&group).unwrap_or(0);

            let value = match &self.source {
                Source::Column(c) => update[*c].clone(),
                Source::Literal(d) => d.clone(),
            };

            let (change, r) = match &mut update {
                RowUpdate::Add(r) => (1, r),
                RowUpdate::Remove(r) => (-1, r),
            };
            let source_change = if value == DataType::None { 0 } else { change };

            self.set_count(group, cur + source_change);
            r.data.push(DataType::Integer(cur + source_change));
        }
        updates
    }
}

struct CountState {

}

// What things are we counting
// This can be a literal value, a value from a column, or a row count, ex: Count(*)
// Row count is identical to count(1) which makes count equal in source to other aggregation functions
// Thus we should translate all count(*) to count(1) before this point.

// If its not in a group its erased, unless there is no group in which case all go through
// Aggregations must also be projections. Where they differ is what you do with the dropped values.
// Projection is the special case of doing nothing. Though may be better to have that be its own node
// as it doesn't require state.
