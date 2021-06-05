use super::data::DataType;
use std::collections::HashMap;

/// State handles stateful interactions for operations
pub trait State {
    fn get(&self, key: &Key) -> Vec<DataType>;

    fn set(&mut self, key: Key, values: Vec<DataType>);
}

pub type Key = Vec<DataType>;

/// MemStore implements state with an in mem hashmap.
pub struct MemStore {
    data: HashMap<Key, Vec<DataType>>,
}

impl MemStore {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }
}

impl State for MemStore {
    fn get(&self, key: &Key) -> Vec<DataType> {
        self.data.get(key).unwrap_or(&vec![]).clone()
    }

    fn set(&mut self, key: Key, values: Vec<DataType>) {
        self.data.insert(key, values);
    }
}
