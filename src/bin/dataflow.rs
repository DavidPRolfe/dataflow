use dataflow::operations::data::{Comparison, DataType, RowUpdate, Source, Updates};
use dataflow::operations::state::MemStore;
use dataflow::operations::{Count, Filter, Map};
use dataflow::processing::worker::DebugWorker;
use dataflow::processing::{Message, MessageRouter, OpWorker};
use std::sync::Arc;
use std::thread;
use std::thread::sleep;
use std::time::Duration;
use dataflow::operations::filter::{ColumnConstraint, Constraint};

fn main() {
    let router = Arc::new(MessageRouter::new());

    let mut worker1 = OpWorker::new(
        router.clone(),
        Filter {
            constraints: vec![ColumnConstraint {
                column: 0,
                constraint: Constraint::Comparison(Comparison::GreaterThan, DataType::Integer(30)),
            }],
        },
        vec![],
    );

    let mut worker2 = OpWorker::new(router.clone(), Map { sources: vec![Source::Column(1)] }, vec![worker1.id]);

    let mut worker3 = OpWorker::new(
        router.clone(),
        Count {
            source: Source::Literal(1.into()),
            group: vec![0],
            state: MemStore::new(),
        },
        vec![worker2.id],
    );

    let mut result_worker = DebugWorker::new(router.clone(), vec![worker3.id]);

    let worker1_id = worker1.id;
    let worker2_id = worker2.id;
    let worker3_id = worker3.id;
    let worker4_id = result_worker.id;

    let thread = thread::spawn(move || {
        worker1.start();
        println!("finished processing thread 1");
    });

    let thread2 = thread::spawn(move || {
        worker2.start();
        println!("finished processing thread 2");
    });

    let thread3 = thread::spawn(move || {
        worker3.start();
        println!("finished processing thread 3");
    });

    let thread4 = thread::spawn(move || {
        result_worker.start();
        println!("finished processing thread 4");
    });

    router.send_message(
        worker1_id,
        Message::Update(Updates {
            updates: vec![RowUpdate::Add(vec![300.into(), true.into()].into())],
            source: 0,
            destination: worker1_id,
        }),
    );
    router.send_message(
        worker1_id,
        Message::Update(Updates {
            updates: vec![RowUpdate::Add(vec![200.into(), true.into()].into())],
            source: 0,
            destination: worker1_id,
        }),
    );
    router.send_message(
        worker1_id,
        Message::Update(Updates {
            updates: vec![RowUpdate::Add(vec![20.into(), true.into()].into())],
            source: 0,
            destination: worker1_id,
        }),
    );
    router.send_message(
        worker1_id,
        Message::Update(Updates {
            updates: vec![RowUpdate::Add(vec![50.into(), false.into()].into())],
            source: 0,
            destination: worker1_id,
        }),
    );

    sleep(Duration::from_secs(10));
    router.send_message(worker1_id, Message::Stop);
    router.send_message(worker2_id, Message::Stop);
    router.send_message(worker3_id, Message::Stop);
    router.send_message(worker4_id, Message::Stop);

    thread.join().unwrap();
    thread2.join().unwrap();
    thread3.join().unwrap();
    thread4.join().unwrap();
    println!("Finished waiting for threads to run")
}
