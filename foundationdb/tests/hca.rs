// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

#![feature(async_await, async_closure)]

use foundationdb;
#[macro_use]
extern crate lazy_static;

use foundationdb::{hca::HighContentionAllocator, *};

use std::{collections::HashSet, iter::FromIterator};

mod common;

#[tokio::test]
async fn test_hca_many_sequential_allocations() -> error::Result<()> {
    common::setup_static();
    const N: usize = 6000;
    const KEY: &[u8] = b"test-hca-allocate";

    let db = common::create_db().await?;

    db.transact(async move |tx| {
        tx.clear_subspace_range(Subspace::from_bytes(KEY));
        error::Result::Ok(())
    })
    .await
    .expect("unable to clear hca test range");

    let hca = HighContentionAllocator::new(Subspace::from_bytes(KEY));

    let mut all_ints = Vec::new();

    for _ in 0..N {
        let mut tx: Transaction = db.create_trx()?;

        let next_int: i64 = hca.allocate(&mut tx).await?;
        all_ints.push(next_int);

        tx.commit().await?;
    }

    check_hca_result_uniqueness(&all_ints);

    eprintln!("ran test {:?}", all_ints);

    Ok(())
}

#[tokio::test]
async fn test_hca_concurrent_allocations() -> error::Result<()> {
    common::setup_static();
    const N: usize = 1000;
    const KEY: &[u8] = b"test-hca-allocate-concurrent";
    let db = common::create_db().await?;

    db.transact(async move |tx| {
        tx.clear_subspace_range(Subspace::from_bytes(KEY));
        error::Result::Ok(())
    })
    .await
    .expect("unable to clear hca test range");

    let mut futures = Vec::new();
    let mut all_ints: Vec<i64> = Vec::new();

    for _ in 0..N {
        let f = db.transact(async move |mut tx| {
            let hca = HighContentionAllocator::new(Subspace::from_bytes(KEY));
            let i = hca.allocate(&mut tx).await?;
            error::Result::Ok(i)
        });

        futures.push(f);
    }

    for allocation in futures {
        let i = allocation.await.expect("unable to get allocation");
        all_ints.push(i);
    }

    check_hca_result_uniqueness(&all_ints);

    eprintln!("ran test {:?}", all_ints);

    Ok(())
}

fn check_hca_result_uniqueness(results: &Vec<i64>) {
    let result_set: HashSet<i64> = HashSet::from_iter(results.clone());

    if results.len() != result_set.len() {
        panic!(
            "Set size does not much, got duplicates from HCA. Set: {:?}, List: {:?}",
            result_set.len(),
            results.len(),
        );
    }
}
