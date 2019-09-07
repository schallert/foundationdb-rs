// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

#![feature(async_await, async_closure)]

use foundationdb as fdb;

#[macro_use]
extern crate lazy_static;

use fdb::*;
use futures::future::*;

mod common;

#[tokio::test]
async fn test_set_get() -> error::Result<()> {
    common::setup_static();
    let db = common::create_db().await?;
    let trx = db.create_trx()?;

    trx.set(b"hello", b"world");
    let trx = trx.commit().await?;

    let trx = trx.database().create_trx()?;

    let res = trx.get(b"hello", false).await?;
    let val = res.value();
    eprintln!("value: {:?}", val);

    let trx = res.transaction();
    trx.clear(b"hello");
    let trx = trx.commit().await?;

    let trx = trx.database().create_trx()?;
    let res = trx.get(b"hello", false).await?;
    eprintln!("value: {:?}", res.value());
    Ok(())
}

#[tokio::test]
async fn test_get_multi() -> fdb::error::Result<()> {
    common::setup_static();
    let db = common::create_db().await?;

    let trx = db.create_trx()?;
    let keys: &[&[u8]] = &[b"hello", b"world", b"foo", b"bar"];

    let futs = keys.iter().map(|k| trx.get(k, false)).collect::<Vec<_>>();
    let results = join_all(futs).await;

    for (i, res) in results.into_iter().enumerate() {
        let res = res?;
        eprintln!("res[{}]: {:?}", i, res.value());
    }

    Ok(())
}

#[tokio::test]
async fn test_set_conflict() -> fdb::error::Result<()> {
    common::setup_static();

    let key = b"test-conflict";
    let db = common::create_db().await?;

    // First transaction. It will be committed before second one.
    let trx1 = db.create_trx()?;
    trx1.set(key, common::random_str(10).as_bytes());
    let trx1_fut = trx1.commit();

    // Second transaction. There will be conflicted by first transaction before commit.
    let trx2 = db.create_trx()?;
    // try to read value to set conflict range
    trx2.get(key, false).await?;
    // commit first transaction to create conflict
    trx1_fut.await?;

    // commit seconds transaction, which will cause conflict
    trx2.set(key, common::random_str(10).as_bytes());
    let res = trx2.commit().await;

    let e = res.expect_err("expected transaction conflict");
    eprintln!("error as expected: {:?}", e);
    // 1020 is fdb error code for transaction conflict.
    assert_eq!(e.code(), 1020);

    Ok(())
}

#[tokio::test]
async fn test_set_conflict_snapshot() -> fdb::error::Result<()> {
    common::setup_static();

    let key = b"test-conflict-snapshot";
    let db = common::create_db().await?;

    // First transaction. It will be committed before second one.
    let trx = db.create_trx()?;
    trx.set(key, common::random_str(10).as_bytes());
    trx.commit().await?;

    // Second transaction.
    let val = db.create_trx()?.get(key, true).await?;
    // snapshot read does not set conflict range, so both transaction will be
    // committed.

    // commit first transaction
    let trx = val.transaction();

    // commit seconds transaction, which will *not* cause conflict because of
    // snapshot read
    trx.set(key, common::random_str(10).as_bytes());
    trx.commit().await?;

    Ok(())
}

// Makes the key dirty. It will abort transactions which performs non-snapshot read on the `key`.
async fn make_dirty(db: &Database, key: &[u8]) {
    let trx = db.create_trx().unwrap();
    trx.set(key, b"");
    trx.commit().await.unwrap();
}

#[tokio::test]
async fn test_transact() -> error::Result<()> {
    use std::sync::{atomic::*, Arc};

    const KEY: &[u8] = b"test-transact";
    const RETRY_COUNT: usize = 5;
    common::setup_static();

    let try_count = Arc::new(AtomicUsize::new(0));
    let try_count0 = &try_count.clone();

    let db = common::create_db().await?;

    // start tranasction with retry
    let res = db
        .transact(async move |trx| {
            let try_count0 = Arc::clone(try_count0);
            // increment try counter
            try_count0.fetch_add(1, Ordering::SeqCst);

            trx.set_option(options::TransactionOption::RetryLimit(RETRY_COUNT as u32))
                .expect("failed to set retry limit");

            let db = trx.database();

            // update conflict range
            let res = trx.get(KEY, false).await?;
            // make current transaction invalid by making conflict
            make_dirty(&db, KEY).await;

            let trx = res.transaction();
            trx.set(KEY, common::random_str(10).as_bytes());
            // `Database::transact` will handle commit by itself, so returns without commit
            error::Result::Ok(())
        })
        .await;

    let e = res.expect_err("should not be able to commit");
    eprintln!("failed as expected: {:?}", e);

    // `TransactionOption::RetryCount` does not count first try, so `try_count` should be equal to
    // `RETRY_COUNT+1`
    assert_eq!(try_count.load(Ordering::SeqCst), RETRY_COUNT + 1);
    Ok(())
}

#[tokio::test]
async fn test_versionstamp() -> error::Result<()> {
    const KEY: &[u8] = b"test-versionstamp";
    common::setup_static();

    let db = common::create_db().await?;

    let trx = db.create_trx()?;

    trx.set(KEY, common::random_str(10).as_bytes());
    // NB(schallert): MUST construct versionstamp future before commit() is polled, but VS future is
    // only ready once commit is complete.
    let vs = trx.get_versionstamp();
    trx.commit().await?;
    let vs = vs.await?;

    eprintln!("versionstamp: {:?}", vs.versionstamp());

    Ok(())
}

#[tokio::test]
async fn test_read_version() -> error::Result<()> {
    common::setup_static();

    let db = common::create_db().await?;
    let trx = db.create_trx()?;
    let v = trx.get_read_version().await?;
    eprintln!("read version: {:?}", v);

    Ok(())
}

#[tokio::test]
async fn test_set_read_version() -> error::Result<()> {
    const KEY: &[u8] = b"test-versionstamp";
    common::setup_static();

    let db = common::create_db().await?;
    let trx = db.create_trx()?;
    trx.set_read_version(0);
    let res = trx.get(KEY, false).await;

    let e = res.expect_err("should fail with past_version");
    eprintln!("failed as expeced: {:?}", e);

    Ok(())
}
