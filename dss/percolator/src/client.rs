use std::thread;
use std::time::Duration;

use crate::service::{TSOClient, TransactionClient};

use crate::msg::{TimestampRequest};

use labrpc::*;
use labrpc::RpcFuture;
use futures::Future;

// BACKOFF_TIME_MS is the wait time before retrying to send the request.
// It should be exponential growth. e.g.
//|  retry time  |  backoff time  |
//|--------------|----------------|
//|      1       |       100      |
//|      2       |       200      |
//|      3       |       400      |
const BACKOFF_TIME_MS: u64 = 100;
// RETRY_TIMES is the maximum number of times a client attempts to send a request.
const RETRY_TIMES: usize = 3;

/// Client mainly has two purposes:
/// One is getting a monotonically increasing timestamp from TSO (Timestamp Oracle).
/// The other is do the transaction logic.
#[derive(Clone)]
pub struct Client {
    tso_client: TSOClient,
    txn_client: TransactionClient
}

impl Client {
    /// Creates a new Client.
    pub fn new(tso_client: TSOClient, txn_client: TransactionClient) -> Client {
        // Your code here.
        Client {
            tso_client,
            txn_client
        }
    }

    fn _get_backoff(&self, attempt: usize) -> u64 {
        BACKOFF_TIME_MS * (1 << attempt)
    }

    /// Gets a timestamp from a TSO.
    pub fn get_timestamp(&self) -> Result<u64> {
        let mut result = self.tso_client.get_timestamp(&TimestampRequest {}).wait();
        for attempt in 0..(RETRY_TIMES - 1) {
            if result.is_ok() {
                break;
            }
            thread::sleep(Duration::from_millis(self._get_backoff(attempt)));

            result = self.tso_client.get_timestamp(&TimestampRequest {}).wait();
        }

        match result {
            Ok(response) => Ok(response.ts),
            Err(err) => Err(err)
        }
    }

    /// Begins a new transaction.
    pub fn begin(&mut self) {
        // Your code here.
        unimplemented!()
    }

    /// Gets the value for a given key.
    pub fn get(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        // Your code here.
        unimplemented!()
    }

    /// Sets keys in a buffer until commit time.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        // Your code here.
        unimplemented!()
    }

    /// Commits a transaction.
    pub fn commit(&self) -> Result<bool> {
        // Your code here.
        unimplemented!()
    }
}
