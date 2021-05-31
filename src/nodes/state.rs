use crate::nodes::data::DataType;
use std::collections::HashMap;

pub trait State {
    fn get(&self, key: &Key) -> Vec<DataType>;

    fn set(&mut self, key: Key, values: Vec<DataType>);
}

/// State handles stateful interactions for nodes
/// This is handles as an in mem hashmap though this will likely turn into a trait later and be
/// implemented with different storage strategies.
pub struct MemStore {
    data: HashMap<Key, Vec<DataType>>
}

impl State for MemStore {
    fn get(&self, key: &Key) -> Vec<DataType> {
        self.data.get(key).unwrap_or(&vec![]).clone()
    }

    fn set(&mut self, key: Key, values: Vec<DataType>) {
        self.data.insert(key, values);
    }
}

pub type Key = Vec<DataType>;