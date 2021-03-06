use ordered_float::OrderedFloat;
use std::ops::{Index, IndexMut};
use std::slice::SliceIndex;

/// DataType exists to make code generic over the supported data types
///
/// Uses ordered floats to make them hashable but as a result don't support NaN's to IEEE standard.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub enum DataType {
    None,
    Integer(i32),
    Text(String),
    Boolean(bool),
    Float(OrderedFloat<f32>),
}

impl From<i32> for DataType {
    fn from(n: i32) -> Self {
        Self::Integer(n)
    }
}

impl From<f32> for DataType {
    fn from(n: f32) -> Self {
        Self::Float(OrderedFloat::from(n))
    }
}

impl From<String> for DataType {
    fn from(n: String) -> Self {
        Self::Text(n)
    }
}

impl From<&str> for DataType {
    fn from(n: &str) -> Self {
        Self::Text(n.into())
    }
}

impl From<bool> for DataType {
    fn from(n: bool) -> Self {
        Self::Boolean(n)
    }
}

/// Comparison is used to hold and perform comparisons of two DataTypes
pub enum Comparison {
    Equal,
    NotEqual,
    GreaterThan,
    LessThan,
    GreaterEqualThan,
    LessEqualThan,
}

impl Comparison {
    /// compares two DataType values based on the op
    pub fn compare(&self, d1: &DataType, d2: &DataType) -> bool {
        match self {
            Comparison::Equal => d1 == d2,
            Comparison::NotEqual => d1 != d2,
            Comparison::GreaterThan => d1 > d2,
            Comparison::LessThan => d1 < d2,
            Comparison::GreaterEqualThan => d1 >= d2,
            Comparison::LessEqualThan => d1 <= d2,
        }
    }
}

/// A single row of data
#[derive(Debug, Clone)]
pub struct Row {
    pub data: Vec<DataType>,
}

impl From<Vec<DataType>> for Row {
    fn from(data: Vec<DataType>) -> Self {
        Self { data }
    }
}

impl<Idx> Index<Idx> for Row
where
    Idx: SliceIndex<[DataType]>,
{
    type Output = Idx::Output;

    fn index(&self, i: Idx) -> &Self::Output {
        &self.data[i]
    }
}

impl<Idx> IndexMut<Idx> for Row
where
    Idx: SliceIndex<[DataType]>,
{
    fn index_mut(&mut self, i: Idx) -> &mut Self::Output {
        &mut self.data[i]
    }
}

/// Used to send how rows have changed
#[derive(Debug, Clone)]
pub enum RowUpdate {
    Add(Row),
    Remove(Row),
}

impl<Idx> Index<Idx> for RowUpdate
where
    Idx: SliceIndex<[DataType]>,
{
    type Output = Idx::Output;

    fn index(&self, i: Idx) -> &Self::Output {
        match self {
            RowUpdate::Add(r) => &r[i],
            RowUpdate::Remove(r) => &r[i],
        }
    }
}

impl<Idx> IndexMut<Idx> for RowUpdate
where
    Idx: SliceIndex<[DataType]>,
{
    fn index_mut(&mut self, i: Idx) -> &mut Self::Output {
        match self {
            RowUpdate::Add(r) => &mut r[i],
            RowUpdate::Remove(r) => &mut r[i],
        }
    }
}

/// Updates store RowUpdates in a vec in case we need to send both an add and remove (say in case of a base row
/// being updated).
#[derive(Debug)]
pub struct Updates {
    pub updates: Vec<RowUpdate>,
    pub source: usize,
    pub destination: usize,
}

pub type Column = usize;

pub enum Source {
    Column(Column),
    Literal(DataType),
}

#[cfg(test)]
mod tests {
    use super::*;

    impl From<Vec<RowUpdate>> for Updates {
        fn from(u: Vec<RowUpdate>) -> Self {
            Self {
                updates: u,
                source: 0,
                destination: 0
            }
        }
    }

    #[test]
    fn equality_works() {
        assert_eq!(
            Comparison::Equal.compare(&DataType::None, &DataType::None),
            true
        );
        assert_eq!(
            Comparison::NotEqual.compare(&DataType::None, &DataType::None),
            false
        );

        assert_eq!(Comparison::Equal.compare(&1.into(), &1.into()), true);
        assert_eq!(Comparison::Equal.compare(&1.into(), &2.into()), false);

        assert_eq!(Comparison::NotEqual.compare(&1.into(), &1.into()), false);
        assert_eq!(Comparison::NotEqual.compare(&1.into(), &2.into()), true);

        assert_eq!(Comparison::Equal.compare(&1.0.into(), &1.0.into()), true);
        assert_eq!(Comparison::Equal.compare(&1.0.into(), &2.0.into()), false);

        assert_eq!(
            Comparison::NotEqual.compare(&1.0.into(), &1.0.into()),
            false
        );
        assert_eq!(Comparison::NotEqual.compare(&1.0.into(), &2.0.into()), true);

        assert_eq!(
            Comparison::Equal.compare(&"Hello There".into(), &"Hello There".into()),
            true
        );
        assert_eq!(
            Comparison::Equal.compare(&"Hello There".into(), &"General Kenobi".into()),
            false
        );

        assert_eq!(
            Comparison::NotEqual.compare(&"Hello There".into(), &"Hello There".into()),
            false
        );
        assert_eq!(
            Comparison::NotEqual.compare(&"Hello There".into(), &"General Kenobi".into()),
            true
        );

        assert_eq!(Comparison::Equal.compare(&true.into(), &true.into()), true);
        assert_eq!(
            Comparison::Equal.compare(&true.into(), &false.into()),
            false
        );

        assert_eq!(
            Comparison::NotEqual.compare(&true.into(), &true.into()),
            false
        );
        assert_eq!(
            Comparison::NotEqual.compare(&true.into(), &false.into()),
            true
        );
    }
}
