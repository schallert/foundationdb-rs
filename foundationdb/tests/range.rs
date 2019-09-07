// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

#![feature(async_await)]

use foundationdb;

#[macro_use]
extern crate lazy_static;

use foundationdb::*;
use futures::{future::*, prelude::*};

mod common;

#[tokio::test]
async fn test_get_range() -> error::Result<()> {
    use foundationdb::keyselector::KeySelector;

    common::setup_static();
    const N: usize = 10000;

    Cluster::new(foundationdb::default_config_path()).await?;
    let db = common::create_db().await?;
    let trx = db.create_trx()?;

    let key_begin = "test-range-";
    let key_end = "test-range.";

    trx.clear_range(key_begin.as_bytes(), key_end.as_bytes());

    for _ in 0..N {
        let key = format!("{}-{}", key_begin, common::random_str(10));
        let value = common::random_str(10);
        trx.set(key.as_bytes(), value.as_bytes());
    }

    let begin = KeySelector::first_greater_or_equal(key_begin.as_bytes());
    let end = KeySelector::first_greater_than(key_end.as_bytes());
    let opt = transaction::RangeOptionBuilder::new(begin, end).build();

    let count = trx
        .get_ranges(opt)
        .try_fold(0, |mut count, item| {
            let kvs = item.key_values();
            count += kvs.as_ref().len();
            ready(Ok(count))
        })
        .await?;

    assert_eq!(count, N);
    eprintln!("count: {:?}", count);

    Ok(())
}
