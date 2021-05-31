use crate::nodes::data::{Column, Comparison, DataType, RowUpdates};
use crate::nodes::Updater;

/// Filter node will remove all rows that don't meet all of the constraint
struct Filter {
    constraints: Vec<ColumnConstraint>,
}

struct ColumnConstraint {
    column: Column,
    constraint: Constraint,
}

enum Constraint {
    Comparison(Comparison, DataType),
    In(Vec<DataType>),
}

impl Updater for Filter {
    fn process(&mut self, mut updates: RowUpdates) -> RowUpdates {
        updates.updates.retain(|update| {
            self.constraints
                .iter()
                .all(|constraint| !match &constraint.constraint {
                    Constraint::Comparison(op, value) => {
                        op.compare(&update[constraint.column], &value)
                    }
                    Constraint::In(values) => values.contains(&update[constraint.column]),
                })
        });

        return updates;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::data::RowUpdate;

    #[test]
    fn filters_nothing() {
        let mut filter = Filter {
            constraints: vec![],
        };
        assert_eq!(filter.process(vec![].into()).updates.len(), 0);
    }

    #[test]
    fn filters_rows() {
        let row_updates = vec![
            RowUpdate::Add(vec![27.into(), "true".into(), 31.into()].into()),
            RowUpdate::Remove(vec![27.into(), "false".into(), 31.into()].into()),
            RowUpdate::Add(vec![27.into(), "not true or false".into(), 31.into()].into()), // Should be only passing row
            RowUpdate::Remove(vec![32.into(), "not true or false".into(), 31.into()].into()),
            RowUpdate::Add(vec![32.into(), "true".into(), 31.into()].into()),
        ];

        let constraints = vec![
            ColumnConstraint {
                column: 0,
                constraint: Constraint::Comparison(Comparison::GreaterThan, DataType::Integer(30)),
            },
            ColumnConstraint {
                column: 1,
                constraint: Constraint::In(vec!["true".into(), "false".into()].into()),
            },
        ];
        let mut filter = Filter { constraints };

        let filtered = filter.process(row_updates.into());
        assert_eq!(filtered.updates.len(), 1);
        assert_eq!(filtered.updates[0][0], 27.into());
        assert_eq!(filtered.updates[0][1], "not true or false".into());
        assert_eq!(filtered.updates[0][2], 31.into());
    }
}
