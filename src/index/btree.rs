use std::{collections::BTreeMap, sync::Arc};

use parking_lot::RwLock;

use crate::data::log_record::LogRecorPos;

pub struct BTree {
    tree: Arc<RwLock<BTreeMap<Vec<u8>, LogRecorPos>>>,
}
