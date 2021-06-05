use crate::operations::Operation;
use crate::router::{MessageRouter, Message};
use std::sync::Arc;

/// Workers use operations to handle incoming messages
pub struct Worker<T: Operation> {
    pub id: usize,
    op: T,
    router: Arc<MessageRouter>,
}

impl<T: Operation> Worker<T> {
    pub fn new(router: Arc<MessageRouter>, op: T, parents: Vec<usize>) -> Self {
        Worker {
            id: router.add_worker(parents),
            op,
            router,
        }
    }

    /// starts running the worker. This will loop until the message router stops providing messages
    pub fn start(&mut self) {
        for message in self.router.iter(self.id) {
            match message {
                Message::Update(u) => {
                    let updates = self.op.process(u);

                    self.router.send_updates(self.id, updates)
                }
                Message::Stop => break,
            }
        }
    }
}