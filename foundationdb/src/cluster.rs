// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Implementations of the FDBCluster C API
//!
//! https://apple.github.io/foundationdb/api-c.html#cluster

use crate::future::*;
use foundationdb_sys as fdb;
use std::{self, sync::Arc};

use crate::{database::*, error::*};

/// An opaque type that represents a Cluster in the FoundationDB C API.
#[derive(Clone)]
pub struct Cluster {
    inner: Arc<ClusterInner>,
}
impl Cluster {
    /// Returns an FdbFuture which will be set to an FDBCluster object.
    ///
    /// # Arguments
    ///
    /// * `path` - A string giving a local path of a cluster file (often called ‘fdb.cluster’) which contains connection information for the FoundationDB cluster. See `foundationdb::default_config_path()`
    ///
    /// TODO: implement Default for Cluster where: If cluster_file_path is NULL or an empty string, then a default cluster file will be used. see
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(path: &str) -> Result<Cluster> {
        let path_str = std::ffi::CString::new(path).unwrap();
        let fut = unsafe {
            let f = fdb::fdb_create_cluster(path_str.as_ptr());
            FdbFuture::new(f)
        };
        let result = fut.await?;
        let cluster = unsafe { result.get_cluster()? };
        Ok(Self {
            inner: Arc::new(ClusterInner::new(cluster)),
        })
    }

    // TODO: fdb_cluster_set_option impl

    /// Returns an `FdbFuture` which will be set to an `Database` object.
    ///
    /// TODO: impl Future
    pub async fn create_database(&self) -> Result<Database> {
        unsafe {
            let f_db = fdb::fdb_cluster_create_database(self.inner.inner, b"DB" as *const _, 2);
            let cluster = self.clone();
            let res = FdbFuture::new(f_db).await?;
            let db = res.get_database()?;
            Ok(Database::new(cluster, db))
        }
    }
}

//TODO: should check if `fdb::FDBCluster` is thread-safe.
struct ClusterInner {
    inner: *mut fdb::FDBCluster,
}
impl ClusterInner {
    fn new(inner: *mut fdb::FDBCluster) -> Self {
        Self { inner }
    }
}
impl Drop for ClusterInner {
    fn drop(&mut self) {
        unsafe {
            fdb::fdb_cluster_destroy(self.inner);
        }
    }
}
unsafe impl Send for ClusterInner {}
unsafe impl Sync for ClusterInner {}
