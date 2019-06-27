use crate::msg::*;
use crate::service::*;
use crate::*;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use std::cell::Cell;
use std::ops::Bound;
use std::time::Duration;

use labrpc::RpcFuture;

use futures::Future;

// TTL is used for a lock key.
// If the key's lifetime exceeds this value, it should be cleaned up.
// Otherwise, the operation should back off.
const TTL: u64 = Duration::from_millis(100).as_nanos() as u64;

#[derive(Clone, Default)]
pub struct TimestampOracle {
    timestamp: Arc<AtomicU64>,
}

impl timestamp::Service for TimestampOracle {
    // example get_timestamp RPC handler.
    fn get_timestamp(&self, _: TimestampRequest) -> RpcFuture<TimestampResponse> {
        let ts = self.timestamp.fetch_add(1, Ordering::SeqCst);
        let resp = TimestampResponse {
            ts
        };
        Box::new(futures::future::result(Ok(resp)))
    }
}

// Key is a tuple (raw key, timestamp).
pub type Key = (Vec<u8>, u64);

#[derive(Clone, PartialEq)]
pub enum Value {
    Timestamp(u64),
    Vector(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct Write(Vec<u8>, Vec<u8>);

pub enum Column {
    Write,
    Data,
    Lock,
}

// KvTable is used to simulate Google's Bigtable.
// It provides three columns: Write, Data, and Lock.
#[derive(Clone, Default)]
pub struct KvTable {
    write: BTreeMap<Key, Value>,
    data: BTreeMap<Key, Value>,
    lock: BTreeMap<Key, Value>,
}

impl KvTable {
    fn column(&self, col: Column) -> &BTreeMap<Key, Value> {
        match col {
            Column::Write => &self.write,
            Column::Data => &self.data,
            Column::Lock => &self.lock,
        }
    }

    fn column_mut(&mut self, col: Column) -> &mut BTreeMap<Key, Value> {
        match col {
            Column::Write => &mut self.write,
            Column::Data => &mut self.data,
            Column::Lock => &mut self.lock,
        }
    }
    // Reads the latest key-value record from a specified column
    // in MemoryStorage with a given key and a timestamp range.
    #[inline]
    fn read(
        &self,
        key: Vec<u8>,
        column: Column,
        ts_start_inclusive: Option<u64>,
        ts_end_inclusive: Option<u64>,
    ) -> Option<(&Key, &Value)> {

        let key_start = ts_start_inclusive
                            .map(|s: u64| Bound::Included((key.clone(), s)))
                            .unwrap_or_else(|| Bound::Unbounded);

        let key_end = ts_end_inclusive
                            .map(|s| Bound::Included((key, s)))
                            .unwrap_or_else(|| Bound::Unbounded);
        let mut resp = self.column(column).range((key_start, key_end));
        resp.rev().next()
    }

    // Writes a record to a specified column in MemoryStorage.
    #[inline]
    fn write(&mut self, key: Vec<u8>, column: Column, ts: u64, value: Value) {
        self.column_mut(column).insert((key, ts), value);
    }

    #[inline]
    // Erases a record from a specified column in MemoryStorage.
    fn erase(&mut self, key: Vec<u8>, column: Column, commit_ts: u64) {
        self.column_mut(column).remove(&(key, commit_ts));
    }
}

// MemoryStorage is used to wrap a KvTable.
// You may need to get a snapshot from it.
#[derive(Clone, Default)]
pub struct MemoryStorage {
    data: Arc<Mutex<KvTable>>,
}

impl transaction::Service for MemoryStorage {
    // example get RPC handler.
    fn get(&self, req: GetRequest) -> RpcFuture<GetResponse> {
        // Your code here.
        unimplemented!()
    }

    // example prewrite RPC handler.
    fn prewrite(&self, req: PrewriteRequest) -> RpcFuture<PrewriteResponse> {
        // Your code here.
        unimplemented!()
    }

    // example commit RPC handler.
    fn commit(&self, req: CommitRequest) -> RpcFuture<CommitResponse> {
        // Your code here.
        unimplemented!()
    }
}

impl MemoryStorage {
    fn back_off_maybe_clean_up_lock(&self, start_ts: u64, key: Vec<u8>) {
        // Your code here.
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_timestamp() {
        let service: TimestampOracle = Default::default();
        let mut res = service.get_timestamp(TimestampRequest {}).wait();

        // First timestamp will be 0.
        assert_eq!(res.unwrap().ts, 0);

        // Next timestamp should be 1.
        res = service.get_timestamp(TimestampRequest {}).wait();
        assert_eq!(res.unwrap().ts, 1);
    }
}
