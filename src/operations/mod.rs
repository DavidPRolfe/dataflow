use self::count::Count;
use self::data::RowUpdates;
use self::filter::Filter;
use self::map::Map;
use self::state::State;

mod count;
pub mod data;
mod filter;
mod map;
pub mod state;

pub enum Op<S: State> {
    Filter(Filter),
    Count(Count<S>),
    Map(Map),
}

impl<S: State> Operation for Op<S> {
    fn process(&mut self, updates: RowUpdates) -> RowUpdates {
        use Op::*;
        match self {
            Filter(n) => n.process(updates),
            Count(n) => n.process(updates),
            Map(n) => n.process(updates),
        }
    }
}

/// An Operation can process any RowUpdates it gets
pub trait Operation {
    /// Process handles any updates that may then be forwarded on to the next node in the graph
    fn process(&mut self, updates: RowUpdates) -> RowUpdates;
}
