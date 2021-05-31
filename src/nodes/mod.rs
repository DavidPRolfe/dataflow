use crate::nodes::data::RowUpdates;

mod state;
mod count;
pub mod data;
mod filter;
mod map;

enum StatelessOperationType {
    Nop,
    Filter,
}

// Stateful operations must keep track of some sort of state between requests
enum StatefulOperationType {
    Count,
}

/// Updaters can process any updates
trait Updater {
    /// Process handles any updates that may then be forwarded on to the next nodes in the graph
    fn process(&mut self, updates: RowUpdates) -> RowUpdates;
}

struct Input;

impl Updater for Input {
    fn process(&mut self, updates: RowUpdates) -> RowUpdates {
        todo!("will just forward on to children nodes")
    }
}

// struct Operation {
//     op: OperationType
// }
//
// impl Updater for Operation {
//     fn process(self, updates: RowUpdates) -> RowUpdates {
//         todo!("implement persistence and processing of each operation")
//     }
// }

struct Output;

impl Updater for Output {
    fn process(&mut self, updates: RowUpdates) -> RowUpdates {
        print!("{:?}", updates);
        return vec![].into();
    }
}
