// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Implementations of the FDBDatabase C API
//!
//! https://apple.github.io/foundationdb/api-c.html#database

use std::{self, sync::Arc};

use foundationdb_sys as fdb;
use futures::future::Future;

use crate::{
    cluster::*,
    error::{self, Error as FdbError, Result},
    options,
    transaction::*,
};

/// Represents a FoundationDB database — a mutable, lexicographically ordered mapping from binary keys to binary values.
///
/// Modifications to a database are performed via transactions.
#[derive(Clone)]
pub struct Database {
    // Order of fields should not be changed, because Rust drops field top-to-bottom (rfc1857), and
    // database should be dropped before cluster.
    inner: Arc<DatabaseInner>,
    cluster: Cluster,
}
impl Database {
    pub(crate) fn new(cluster: Cluster, db: *mut fdb::FDBDatabase) -> Self {
        let inner = Arc::new(DatabaseInner::new(db));
        Self { cluster, inner }
    }

    /// Called to set an option an on `Database`.
    pub fn set_option(&self, opt: options::DatabaseOption) -> Result<()> {
        unsafe { opt.apply(self.inner.inner) }
    }

    /// Creates a new transaction on the given database.
    pub fn create_trx(&self) -> Result<Transaction> {
        unsafe {
            let mut trx: *mut fdb::FDBTransaction = std::ptr::null_mut();
            error::eval(fdb::fdb_database_create_transaction(
                self.inner.inner,
                &mut trx as *mut _,
            ))?;
            Ok(Transaction::new(self.clone(), trx))
        }
    }

    /// `transact` returns a future which retries on error. It tries to resolve a future created by
    /// caller-provided function `f` inside a retry loop, providing it with a newly created
    /// transaction. After caller-provided future resolves, the transaction will be committed
    /// automatically.
    ///
    /// # Warning
    ///
    /// It might retry indefinitely if the transaction is highly contentious. It is recommended to
    /// set `TransactionOption::RetryLimit` or `TransactionOption::SetTimeout` on the transaction
    /// if the task need to be guaranteed to finish.
    pub async fn transact<F, Fut, Item, Error>(&self, mut f: F) -> Fut::Output
    where
        F: FnMut(Transaction) -> Fut,
        Fut: Future<Output = std::result::Result<Item, Error>>,
        Error: From<FdbError>,
    {
        let db = self.clone();
        let trx = db.create_trx()?;
        loop {
            let trx = trx.clone();
            let res = f(trx.clone()).await?;
            match trx.commit().await {
                Ok(_) => return Ok(res),
                Err(e) => {
                    if e.should_retry() {
                        continue;
                    } else {
                        return Err(Error::from(e));
                    }
                }
            }
        }
    }
}

struct DatabaseInner {
    inner: *mut fdb::FDBDatabase,
}
impl DatabaseInner {
    fn new(inner: *mut fdb::FDBDatabase) -> Self {
        Self { inner }
    }
}
impl Drop for DatabaseInner {
    fn drop(&mut self) {
        unsafe {
            fdb::fdb_database_destroy(self.inner);
        }
    }
}
unsafe impl Send for DatabaseInner {}
unsafe impl Sync for DatabaseInner {}
