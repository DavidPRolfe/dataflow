use crate::operations::Operation;
use crate::processing::router::MessageRouter;
use crate::processing::Message;
use std::sync::Arc;

/// OpWorkers use operations to handle incoming messages
pub struct OpWorker<T: Operation> {
    pub id: usize,
    op: T,
    router: Arc<MessageRouter>,
}

impl<T: Operation> OpWorker<T> {
    pub fn new(router: Arc<MessageRouter>, op: T, parents: Vec<usize>) -> Self {
        Self {
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

/// DebugWorkers just print and forward along incoming messages
pub struct DebugWorker {
    pub id: usize,
    router: Arc<MessageRouter>,
}

impl DebugWorker {
    pub fn new(router: Arc<MessageRouter>, parents: Vec<usize>) -> Self {
        Self {
            id: router.add_worker(parents),
            router,
        }
    }

    /// starts running the worker. This will loop until the message router stops providing messages
    pub fn start(&mut self) {
        for message in self.router.iter(self.id) {
            match message {
                Message::Update(u) => {
                    println!("{:?}", u);
                    self.router.send_updates(self.id, u.updates)
                }
                Message::Stop => {
                    println!("Stopping due to message"); // This should never happen. Stop messages are checked in the iter
                    break;
                }
            }
        }
    }
}
