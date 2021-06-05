use super::data::{DataType, RowUpdate, Updates, Source};
use super::state::State;
use super::Operation;

/// Count is used to get the non-distinct count of rows with non null values passing through it.
/// It can be optionally be grouped by any number of columns.
///
/// Note this doesn't directly support Count(*) from SQL. Instead this must be translated
/// to Count(1) before it gets to this node.
pub struct Count<S: State> {
    pub source: Source,
    pub group: Vec<usize>,
    pub state: S,
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

impl<S: State> Operation for Count<S> {
    // TODO: Group updates and don't send out update if addition/removal cancel out
    fn process(&mut self, mut updates: Updates) -> Vec<RowUpdate> {
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
        updates.updates
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::operations::state::MemStore;

    #[test]
    fn counts_literals() {
        let mut node = Count {
            source: Source::Literal(DataType::Integer(1)),
            group: vec![],
            state: MemStore::new(),
        };
        let updates = vec![
            RowUpdate::Add(vec![0.into(), "hello".into(), DataType::None].into()),
            RowUpdate::Add(vec![1.into(), "hello".into(), DataType::None].into()),
            RowUpdate::Remove(vec![0.into(), "hello".into(), DataType::None].into()),
        ];

        let processed = node.process(updates.into());
        assert_eq!(processed.updates.len(), 3);
        assert_eq!(processed.updates[0][3], 1.into());
        assert_eq!(processed.updates[1][3], 2.into());
        assert_eq!(processed.updates[2][3], 1.into());

        // Checking it works with non-1 literal
        let mut node = Count {
            source: Source::Literal(DataType::Integer(2)),
            group: vec![],
            state: MemStore::new(),
        };

        let updates = vec![
            RowUpdate::Add(vec![0.into(), "hello".into(), DataType::None].into()),
            RowUpdate::Add(vec![1.into(), "hello".into(), DataType::None].into()),
            RowUpdate::Remove(vec![0.into(), "hello".into(), DataType::None].into()),
        ];

        let processed = node.process(updates.into());
        assert_eq!(processed.updates.len(), 3);
        assert_eq!(processed.updates[0][3], 1.into());
        assert_eq!(processed.updates[1][3], 2.into());
        assert_eq!(processed.updates[2][3], 1.into());
    }

    #[test]
    fn counts_null_literal() {
        let mut node = Count {
            source: Source::Literal(DataType::None),
            group: vec![],
            state: MemStore::new(),
        };
        let updates = vec![
            RowUpdate::Add(vec![0.into(), "hello".into(), DataType::None].into()),
            RowUpdate::Add(vec![1.into(), "hello".into(), DataType::None].into()),
            RowUpdate::Remove(vec![0.into(), "hello".into(), DataType::None].into()),
        ];

        let processed = node.process(updates.into());
        assert_eq!(processed.updates.len(), 3);
        assert_eq!(processed.updates[0][3], 0.into());
        assert_eq!(processed.updates[1][3], 0.into());
        assert_eq!(processed.updates[2][3], 0.into());
    }

    #[test]
    fn counts_columns() {
        let mut node = Count {
            source: Source::Column(0),
            group: vec![],
            state: MemStore::new(),
        };
        let updates = vec![
            RowUpdate::Add(vec![0.into(), "hello".into(), DataType::None].into()),
            RowUpdate::Add(vec![1.into(), "hello".into(), DataType::None].into()),
            RowUpdate::Remove(vec![0.into(), "hello".into(), DataType::None].into()),
        ];

        let processed = node.process(updates.into());
        assert_eq!(processed.updates.len(), 3);
        assert_eq!(processed.updates[0][3], 1.into());
        assert_eq!(processed.updates[1][3], 2.into());
        assert_eq!(processed.updates[2][3], 1.into());

        let mut node = Count {
            source: Source::Column(2),
            group: vec![],
            state: MemStore::new(),
        };

        let updates = vec![
            RowUpdate::Add(vec![0.into(), "hello".into(), DataType::None].into()),
            RowUpdate::Add(vec![1.into(), "hello".into(), DataType::None].into()),
            RowUpdate::Remove(vec![0.into(), "hello".into(), DataType::None].into()),
        ];

        let processed = node.process(updates.into());
        assert_eq!(processed.updates.len(), 3);
        assert_eq!(processed.updates[0][3], 0.into());
        assert_eq!(processed.updates[1][3], 0.into());
        assert_eq!(processed.updates[2][3], 0.into());
    }

    #[test]
    fn groups_by_correctly() {
        let mut node = Count {
            source: Source::Column(0),
            group: vec![0],
            state: MemStore::new(),
        };
        let updates = vec![
            RowUpdate::Add(vec![0.into(), "hello".into(), DataType::None].into()),
            RowUpdate::Add(vec![1.into(), "hello".into(), DataType::None].into()),
            RowUpdate::Remove(vec![0.into(), "hello".into(), DataType::None].into()),
        ];

        let processed = node.process(updates.into());
        assert_eq!(processed.updates.len(), 3);
        assert_eq!(processed.updates[0][3], 1.into());
        assert_eq!(processed.updates[1][3], 1.into());
        assert_eq!(processed.updates[2][3], 0.into());

        let mut node = Count {
            source: Source::Column(2),
            group: vec![0],
            state: MemStore::new(),
        };

        let updates = vec![
            RowUpdate::Add(vec![0.into(), "hello".into(), DataType::None].into()),
            RowUpdate::Add(vec![1.into(), "hello".into(), DataType::None].into()),
            RowUpdate::Remove(vec![0.into(), "hello".into(), DataType::None].into()),
        ];

        let processed = node.process(updates.into());
        assert_eq!(processed.updates.len(), 3);
        assert_eq!(processed.updates[0][3], 0.into());
        assert_eq!(processed.updates[1][3], 0.into());
        assert_eq!(processed.updates[2][3], 0.into());
    }
}
