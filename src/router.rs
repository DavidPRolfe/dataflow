use petgraph::stable_graph::{StableGraph, NodeIndex};
use std::sync::RwLock;
use crate::operations::data::{Updates, RowUpdate};

// MessageRouter handles sending and receiving messages
pub struct MessageRouter {
    graph: RwLock<StableGraph<(), ()>>,
}

impl MessageRouter {
    pub fn new() -> Self {
        Self { graph: RwLock::new(StableGraph::new()) }
    }

    /// Add_node handles adding a new worker to the dataflow graph
    pub fn add_worker(&self, parents: Vec<usize>) -> usize {
        let mut graph = self.graph.write().unwrap(); // Fine with writes panicking if the lock is poisoned
        let idx = graph.add_node(());
        for parent in parents {
            graph.add_edge(NodeIndex::new(parent), idx, ());
        }

        idx.index()
    }

    /// next_message waits for the next message for the given worker id
    pub fn next_message(&self, id: usize) -> Message {
        // TODO: Actually receive messages
        return Message::Stop
    }

    /// send_updates sends the RowUpdates to all children of the worker
    pub fn send_updates(&self, id: usize, updates: Vec<RowUpdate>) {
        // TODO: Actually send messages
        if updates.is_empty() {
            return
        }
    }

    pub fn iter(&self, id: usize) -> MessageRouterIter {
        MessageRouterIter{ router: self, worker_id: id }
    }
}

pub struct MessageRouterIter<'a> {
    router: &'a MessageRouter,
    worker_id: usize,
}

impl Iterator for MessageRouterIter<'_> {
    type Item = Message;

    fn next(&mut self) -> Option<Self::Item> {
        let message = self.router.next_message(self.worker_id);
        match message {
            Message::Stop => None,
            _ => Some(message),
        }
    }
}

pub enum Message {
    Update(Updates),
    Stop
}