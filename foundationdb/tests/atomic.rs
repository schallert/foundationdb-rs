// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

#![feature(async_await)]

use byteorder;
use foundationdb;

#[macro_use]
extern crate lazy_static;

use byteorder::ByteOrder;
use foundationdb::*;
use futures::future::*;

mod common;

//TODO: impl Future
async fn atomic_add(db: Database, key: &[u8], value: i64) -> error::Result<()> {
    let trx = db.create_trx()?;

    let val = {
        let mut buf = [0u8; 8];
        byteorder::LE::write_i64(&mut buf, value);
        buf
    };
    trx.atomic_op(key, &val, options::MutationType::Add);
    trx.commit().await?;

    Ok(())
}

#[tokio::test]
async fn test_atomic() -> error::Result<()> {
    common::setup_static();
    const KEY: &[u8] = b"test-atomic";

    let db = common::create_db().await?;
    let trx = db.create_trx()?;
    // clear key before run example
    trx.clear(KEY);
    let trx = trx.commit().await?;
    let db = trx.database();
    let n = 1000usize;

    // Run `n` add(1) operations in parallel
    let db0 = db.clone();
    let fut_add_list = (0..n)
        .map(move |_| atomic_add(db0.clone(), KEY, 1))
        .collect::<Vec<_>>();
    let fut_add = join_all(fut_add_list);

    // Run `n` add(-1) operations in parallel
    let db0 = db.clone();
    let fut_sub_list = (0..n)
        .map(move |_| atomic_add(db0.clone(), KEY, -1))
        .collect::<Vec<_>>();
    let fut_sub = join_all(fut_sub_list);

    // Wait for all atomic operations
    join(fut_add, fut_sub).await;

    let trx = db.create_trx()?;
    let res = trx.get(KEY, false).await?;
    let val = res.value().expect("value should exist");

    // A value should be zero, as same number of atomic add/sub operations are done.
    let v: i64 = byteorder::LE::read_i64(&val);
    assert_eq!(v, 0);

    Ok(())
}
