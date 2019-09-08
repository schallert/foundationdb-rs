// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

#![feature(async_await)]

use foundationdb as fdb;

#[macro_use]
extern crate lazy_static;

use fdb::*;

mod common;

#[tokio::test]
async fn test_watch() -> error::Result<()> {
    common::setup_static();
    const KEY: &[u8] = b"test-watch";

    let db = common::create_db().await?;

    let trx = db.create_trx()?;
    eprintln!("setting watch");
    let watch = trx.watch(KEY);
    trx.commit().await?;
    eprintln!("watch committed");

    let trx2 = db.create_trx()?;
    eprintln!("writing value");

    let value = common::random_str(10);
    trx2.set(KEY, value.as_bytes());
    trx2.commit().await?;
    eprintln!("write committed");

    // 1. Setup a watch with a key
    // 2. After the watch is installed, try to update the key.
    // 3. After updating the key, waiting for the watch
    watch.await?;
    Ok(())
}

#[tokio::test]
async fn test_watch_without_commit() -> error::Result<()> {
    common::setup_static();
    const KEY: &[u8] = b"test-watch-2";

    let db = common::create_db().await?;
    let trx = db.create_trx()?;
    eprintln!("setting watch");

    // trx will be dropped without `commit`, so a watch will be canceled
    let watch = trx.watch(KEY);
    drop(trx);
    let watch = watch.await;

    // should return error_code=1025, `Operation aborted because the transaction was
    // canceled`
    let e = watch.expect_err("should be cancelled");
    eprintln!("error as expected: {:?}", e);

    Ok(())
}
