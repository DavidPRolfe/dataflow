use crate::operations::Operation;

/// Node handles message routing for its operation
struct Node<T: Operation> {
    op: T
}
