use crate::operations::data::Updates;

pub use self::router::MessageRouter;
pub use self::worker::OpWorker;

pub mod router;
pub mod worker;

pub enum Message {
    Update(Updates),
    Stop,
}
