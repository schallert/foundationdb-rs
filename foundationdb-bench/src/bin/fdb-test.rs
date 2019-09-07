#![feature(async_await)]

extern crate foundationdb as fdb;

#[macro_use]
extern crate log;
use env_logger;
use structopt;

use std::sync::Arc;

use futures::future::*;
use structopt::StructOpt;

use tokio::runtime::Builder;

use crate::fdb::*;

#[derive(StructOpt, Debug, Clone)]
#[structopt(name = "fdb-bench")]
struct Opt {
    #[structopt(short = "t", long = "threads", default_value = "1")]
    threads: usize,

    #[structopt(short = "q", long = "queue-depth", default_value = "1000")]
    queue_depth: usize,

    #[structopt(short = "c", long = "count", default_value = "300000")]
    count: usize,

    #[structopt(long = "trx-batch-size", default_value = "10")]
    trx_batch_size: usize,

    #[structopt(long = "key-len", default_value = "10")]
    key_len: usize,
    #[structopt(long = "val-len", default_value = "100")]
    val_len: usize,
}

fn main() {
    env_logger::init();
    let opt = Opt::from_args();

    info!("opt: {:?}", opt);

    let rt = Builder::new()
        .build()
        .expect("failed to build tokio runtime");

    let network = fdb::init().expect("failed to init network");

    let handle = std::thread::spawn(move || {
        let error = network.run();

        if let Err(error) = error {
            panic!("fdb_run_network: {}", error);
        }
    });

    network.wait();

    rt.block_on(async {
        let cluster_path = fdb::default_config_path();
        let cluster = Cluster::new(cluster_path)
            .await
            .expect("failed to create cluster");

        let db = Arc::new(
            cluster
                .create_database()
                .await
                .expect("failed to get database"),
        );

        let mut handles = Vec::new();
        for i in 0..opt.count {
            let s = format!("foo-{}", i);
            let db = db.clone();
            handles.push(async move {
                let s = s.as_bytes();
                let trx = db.create_trx().unwrap();
                trx.set(s, s);
                trx.commit().await.unwrap();
            });
        }

        join_all(handles).await;
        println!("put foo at bar");
    });

    network.stop().expect("failed to stop network");
    handle.join().expect("failed to join fdb thread");
}
