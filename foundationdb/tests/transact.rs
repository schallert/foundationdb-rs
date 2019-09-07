// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

#![feature(async_await, async_closure)]

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate failure;

mod common;

#[tokio::test]
async fn test_transact_error() -> Result<(), failure::Error> {
    common::setup_static();
    let db = common::create_db().await?;

    let res = db
        .transact(async move |_| -> Result<(), failure::Error> { bail!("failed") })
        .await;

    assert!(res.is_err());

    Ok(())
}
