pub use self::count::Count;
use self::data::Updates;
pub use self::filter::Filter;
pub use self::map::Map;
pub use self::state::State;
use crate::operations::data::RowUpdate;

mod count;
pub mod data;
pub mod filter;
mod map;
pub mod state;

/// An Operation can process any RowUpdates it gets
pub trait Operation {
    /// Process handles any updates that may then be forwarded on to the next node in the graph
    fn process(&mut self, updates: Updates) -> Vec<RowUpdate>;
}
