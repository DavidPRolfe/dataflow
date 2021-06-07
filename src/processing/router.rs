use crate::operations::data::{RowUpdate, Updates};
use crate::processing::Message;
use crossbeam::channel::bounded;
use crossbeam::channel::{Receiver, Sender};
use petgraph::stable_graph::{NodeIndex, StableGraph};
use petgraph::Direction;
use std::collections::HashMap;
use std::sync::RwLock;

/// MessageRouter handles sending and receiving messages
pub struct MessageRouter {
    graph: RwLock<StableGraph<(), ()>>,
    channels: RwLock<HashMap<usize, (Sender<Message>, Receiver<Message>)>>,
}

impl MessageRouter {
    pub fn new() -> Self {
        Self {
            graph: RwLock::new(StableGraph::new()),
            channels: RwLock::new(HashMap::new()),
        }
    }

    /// Add_node handles adding a new worker to the dataflow graph
    pub fn add_worker(&self, parents: Vec<usize>) -> usize {
        let mut graph = self.graph.write().unwrap(); // Fine with writes panicking if the lock is poisoned
        let idx = graph.add_node(());
        for parent in parents {
            graph.add_edge(NodeIndex::new(parent), idx, ());
        }

        let index = idx.index();

        let chan = bounded::<Message>(10);
        self.channels.write().unwrap().insert(index, chan);

        index
    }

    /// next_message waits for the next message for the given worker id
    pub fn next_message(&self, id: usize) -> Message {
        let map = self.channels.read().unwrap(); // Fine with panicking on thread poisoning
        let r = match map.get(&id) {
            None => return Message::Stop, // Worker doesn't exist in channels. It must have been removed so we should stop
            Some((_, r)) => r,
        };

        match r.recv() {
            Ok(m) => m,
            Err(_) => Message::Stop, // Channel has been disconnected so we should stop
        }
    }

    /// send_updates sends the RowUpdates to all children of the worker
    pub fn send_updates(&self, id: usize, updates: Vec<RowUpdate>) {
        if updates.is_empty() {
            return;
        }

        let graph = self.graph.read().unwrap(); // Fine with panicking on thread poisoning
        let children = graph.neighbors_directed(NodeIndex::new(id), Direction::Outgoing);

        for child in children {
            self.send_message(
                child.index(),
                Message::Update(Updates {
                    updates: updates.clone(),
                    source: id,
                    destination: child.index(),
                }),
            );
        }
    }

    pub fn send_message(&self, destination: usize, message: Message) {
        let map = self.channels.read().unwrap(); // Fine with panicking on thread poisoning
        let s = match map.get(&destination) {
            None => return, // Node doesn't exist in map. We'll skip sending this message
            Some((s, _)) => s,
        };

        let _ = s.send(message); // We don't care about if the channel has been disconnected so can ignore the error
    }

    pub fn iter(&self, id: usize) -> MessageRouterIter {
        MessageRouterIter {
            router: self,
            worker_id: id,
        }
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
