use tokio::runtime::Runtime;
use futures::future::{self};
use executors::crossbeam_channel_pool::ThreadPool;
use executors::FuturesExecutor;
use executors::Executor;
use futures::executor::block_on;

use bencher::{benchmark_group, benchmark_main, Bencher};

const BLOCK_COUNT:usize = 1_000;
fn tokio_run_task(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");

    b.iter(|| {
        let task = future::lazy(|_| () );
        runtime.block_on(task);
    });
}

fn futures_run_task(b: &mut Bencher) {
    b.iter(|| {
        let task = future::lazy(|_| () );
        block_on(task);
    });
}

fn spawn_tokio(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");

    b.iter(|| {

        let mut tasks = Vec::with_capacity(BLOCK_COUNT);
        for _i in 0 .. BLOCK_COUNT {
            let task = future::lazy(|_| () );

            tasks.push(runtime.spawn(task));
        }

        runtime.block_on(future::join_all(tasks));
    });
}

fn spawn_smol(b: &mut Bencher) {

    b.iter(|| {

        let mut tasks = Vec::with_capacity(BLOCK_COUNT);
        for _i in 0 .. BLOCK_COUNT {
            let task = future::lazy(|_| () );

            tasks.push(smol::spawn(task));
        }

        smol::block_on(future::join_all(tasks));
    });
}

fn spawn_execrs(b: &mut Bencher) {
    let n_workers = 4;
    let pool = ThreadPool::new(n_workers);

    b.iter(|| {

        let mut tasks = Vec::with_capacity(BLOCK_COUNT);
        for _i in 0 .. BLOCK_COUNT {
            let task = future::lazy(|_| () );

            tasks.push(pool.spawn(task));
        }

        block_on(future::join_all(tasks));
    });
    pool.shutdown().expect("shutdown");
}

benchmark_group!(sched, 
    tokio_run_task,
    futures_run_task,
    spawn_execrs, 
    spawn_smol,
    spawn_tokio);

benchmark_main!(sched);

