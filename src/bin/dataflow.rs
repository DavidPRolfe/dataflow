use dataflow::operations::{Filter, Map, Count};
use std::thread;
use std::thread::sleep;
use std::time::Duration;
use std::sync::Arc;
use dataflow::router::MessageRouter;
use dataflow::worker::Worker;
use dataflow::operations::state::MemStore;
use dataflow::operations::data::Source;


fn main() {
    let router = Arc::new(MessageRouter::new());

    let mut worker1 = Worker::new(router.clone(), Filter {
        constraints: vec![],
    }, vec![]);

    let mut worker2 = Worker::new(router.clone(), Map { sources: vec![] }, vec![worker1.id]);

    let mut worker3 = Worker::new(router.clone(), Count {
        source: Source::Literal(1.into()),
        group: vec![],
        state: MemStore::new(),
    }, vec![worker2.id]);

    let thread = thread::spawn(move || {
        worker1.start();
        sleep(Duration::from_secs(2));
        println!("finished processing thread 1");
    });

    let thread2 = thread::spawn(move || {
        worker2.start();
        sleep(Duration::from_secs(2));
        println!("finished processing thread 2");
    });

    let thread3 = thread::spawn(move || {
        worker3.start();
        sleep(Duration::from_secs(2));
        println!("finished processing thread 2");
    });

    thread.join().unwrap();
    thread2.join().unwrap();
    thread3.join().unwrap();
}
