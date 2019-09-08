#![feature(async_await)]

use env_logger;
use log::info;
use structopt;

use std::sync::{atomic::*, Arc};

use futures::future::*;
use rand::{prelude::*, rngs::mock::StepRng};
use stopwatch::Stopwatch;
use structopt::StructOpt;

use tokio::runtime::Builder;

use foundationdb::{self as fdb, error::*, *};

#[derive(Clone)]
struct Counter {
    size: usize,
    inner: Arc<AtomicUsize>,
}
impl Counter {
    fn new(size: usize) -> Self {
        Self {
            size,
            inner: Default::default(),
        }
    }

    fn decr(&self, n: usize) -> bool {
        let val = self.inner.fetch_add(n, Ordering::SeqCst);
        val < self.size
    }
}

struct BenchRunner {
    #[allow(unused)]
    db: Database,
    counter: Counter,
    key_buf: Vec<u8>,
    val_buf: Vec<u8>,
    rng: StepRng,
    trx: Option<Transaction>,
    trx_batch_size: usize,
}

impl BenchRunner {
    fn new(db: Database, rng: StepRng, counter: Counter, opt: &Opt) -> Self {
        let key_buf = vec![0; opt.key_len];
        let val_buf = vec![0; opt.val_len];

        let trx = db.create_trx().expect("failed to create trx");

        Self {
            db,
            counter,
            key_buf,
            val_buf,

            rng,
            trx: Some(trx),
            trx_batch_size: opt.trx_batch_size,
        }
    }

    async fn run(mut self) -> Result<()> {
        let mut trx = self.trx.take().unwrap();
        loop {
            for _ in 0..self.trx_batch_size {
                self.rng.fill_bytes(&mut self.key_buf);
                self.rng.fill_bytes(&mut self.val_buf);
                self.key_buf[0] = 0x01;
                trx.set(&self.key_buf, &self.val_buf);
            }

            trx = trx.commit().await?;
            trx.reset();
            if !self.counter.decr(self.trx_batch_size) {
                return Ok(());
            }
        }
    }
}

#[derive(Clone)]
struct Bench {
    db: Database,
    opt: Opt,
}

impl Bench {
    async fn run(self) {
        let opt = &self.opt;
        let counter = Counter::new(opt.count);

        let sw = Stopwatch::start_new();

        let step = (opt.queue_depth + opt.threads - 1) / opt.threads;

        let start = 0;
        let end = std::cmp::min(start + step, opt.queue_depth);
        let range = start..end;
        let counter = counter.clone();
        let b = self.clone();

        b.run_range(range, counter)
            .await
            .expect("error running range");

        let elapsed = sw.elapsed_ms() as usize;

        info!(
            "bench took: {:?} ms, {:?} tps",
            elapsed,
            1000 * opt.count / elapsed
        );
    }

    async fn run_range(&self, r: std::ops::Range<usize>, counter: Counter) -> Result<()> {
        let runners = r
            .map(|n| {
                // With deterministic Rng, benchmark with same parameters will overwrite same set
                // of keys again, which makes benchmark result stable.
                let rng = StepRng::new(n as u64, 1);
                BenchRunner::new(self.db.clone(), rng, counter.clone(), &self.opt).run()
            })
            .collect::<Vec<_>>();

        join_all(runners).await;
        Ok(())
    }
}

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
        .core_threads(opt.threads)
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

        let db = cluster
            .create_database()
            .await
            .expect("failed to get database");

        let bench = Bench { db, opt };
        bench.run().await;
    });

    network.stop().expect("failed to stop network");
    handle.join().expect("failed to join fdb thread");
}
