use super::data::{Column, Comparison, DataType, Updates};
use super::Operation;
use crate::operations::data::RowUpdate;

/// Filter will remove all rows that don't meet all of the constraint
pub struct Filter {
    pub constraints: Vec<ColumnConstraint>,
}

pub struct ColumnConstraint {
    pub column: Column,
    pub constraint: Constraint,
}

pub enum Constraint {
    Comparison(Comparison, DataType),
    In(Vec<DataType>),
}

impl Operation for Filter {
    fn process(&mut self, mut updates: Updates) -> Vec<RowUpdate> {
        updates.updates.retain(|update| {
            self.constraints
                .iter()
                .all(|constraint| !match &constraint.constraint {
                    Constraint::Comparison(op, value) => {
                        !op.compare(&update[constraint.column], &value)
                    }
                    Constraint::In(values) => values.contains(&update[constraint.column]),
                })
        });

        return updates.updates;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::data::RowUpdate;
    use crate::processing::Message::Update;

    #[test]
    fn filters_nothing() {
        let mut filter = Filter {
            constraints: vec![],
        };
        assert_eq!(filter.process(vec![].into()).len(), 0);
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
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0][0], 32.into());
        assert_eq!(filtered[0][1], "not true or false".into());
        assert_eq!(filtered[0][2], 31.into());
    }
}
