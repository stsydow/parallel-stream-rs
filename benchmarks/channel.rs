use std::time::Instant;

use tokio::runtime::Runtime;
use futures::future::{FutureExt, TryFutureExt};
use futures::stream::{self, Stream, StreamExt};
use bencher::{benchmark_group, benchmark_main, Bencher, black_box};

use parallel_stream::channel;
use parallel_stream::StreamExt as MyStreamExt;

const BLOCK_COUNT:usize = 1_000;

type Message = usize;

fn new_message(val:usize) -> Message {
    val
}

fn test_msg(m:Message) {
    black_box(m);
}

fn dummy_iter() -> impl Iterator<Item=Message> {
        (0 .. BLOCK_COUNT).map(|i| new_message(i))
}

fn dummy_stream() -> impl Stream<Item=Message> {
    stream::iter(
        dummy_iter()
    )
}

fn time_stream() -> impl Stream<Item=Instant> {
    stream::unfold(0, |count| async move {
        if count < BLOCK_COUNT {
            let count = count + 1;
            let t = Instant::now();
            Some((t, count))
        } else {
            None
        }
    })
}

fn channel_setup(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
        let (tx, mut rx) = channel::<Message>(1);

        futures::stream::once(async {new_message(1)})
        .forward_and_spawn(tx, &runtime);

        let recv_task = || async move { 
            if let Some(item ) = rx.next().await {
                test_msg(item);
            }
        };


        runtime.block_on(recv_task());
    });
}

fn channel_buf1(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
        let (tx, rx) = channel::<Message>(1);

        dummy_stream().forward_and_spawn(tx, &runtime);
        let recv_task = rx
            .for_each(|item| async move {
                test_msg(item);
            });


        runtime.block_on(recv_task);
    });
}

fn smol_channel_buf1(b: &mut Bencher) {
    b.iter(|| {
        let (tx, rx) = channel::<Message>(1);

        smol::spawn(MyStreamExt::forward(dummy_stream(), tx)).detach();

        let recv_task = rx
            .for_each(|item| async move {
                test_msg(item);
            });
        smol::block_on(recv_task);
    });
}

fn execrs_channel_buf1(b: &mut Bencher) {
    use executors::crossbeam_channel_pool::ThreadPool;
    use executors::FuturesExecutor;
    use executors::Executor;
    use futures::executor::block_on;
    let n_workers = 4;
    let pool = ThreadPool::new(n_workers);

    b.iter(|| {
        let (tx, rx) = channel::<Message>(1);

        pool.spawn(MyStreamExt::forward(dummy_stream(), tx)).detach();

        let recv_task = rx
            .for_each(|item| async move {
                test_msg(item);
            });

        let recv = pool.spawn(recv_task);

        block_on(recv);
    });
    pool.shutdown().expect("shutdown");
}

fn channel_buf_big(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
        let (tx, rx) = channel::<Message>(BLOCK_COUNT);

        dummy_stream().forward_and_spawn(tx,&runtime);

        let recv_task = rx
            .for_each(|item| async move {
                test_msg(item);
            });

        runtime.block_on(recv_task);
    });
}

fn smol_channel_buf_big(b: &mut Bencher) {
    b.iter(|| {
        let (tx, rx) = channel::<Message>(BLOCK_COUNT);

        smol::spawn(MyStreamExt::forward(dummy_stream(), tx)).detach();

        let recv_task = rx
            .for_each(|item| async move {
                test_msg(item);
            });
        smol::block_on(recv_task);
    });
}

fn execrs_channel_buf_big(b: &mut Bencher) {
    use executors::crossbeam_channel_pool::ThreadPool;
    use executors::FuturesExecutor;
    use executors::Executor;
    use futures::executor::block_on;
    let n_workers = 4;
    let pool = ThreadPool::new(n_workers);

    b.iter(|| {
        let (tx, rx) = channel::<Message>(BLOCK_COUNT);

        pool.spawn(MyStreamExt::forward(dummy_stream(), tx)).detach();

        let recv_task = rx
            .for_each(|item| async move {
                test_msg(item);
            });

        let recv = pool.spawn(recv_task);

        block_on(recv);
    });
    pool.shutdown().expect("shutdown");
}

fn channel_buf1_flume_fold(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
        let (send_task, recv_task) = {
        let (tx, rx) = flume::bounded::<Message>(1);

        let send_task = dummy_stream()
            .fold(tx, |tx, item| async move {
                let r = tx.send_async(item).await;
                r.expect("Receiver closed");
                tx
            });

        let recv_task = || async move { 
            while let Ok(item) = rx.recv_async().await {
                test_msg(item);
            }
        };
        (send_task, recv_task())
        };
        runtime.spawn(send_task);
        runtime.block_on(recv_task);
    });
}

fn channel_buf1_flume(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
        let (send_task, recv_task) = {
        let (tx, rx) = flume::bounded::<Message>(1);

        let send_task = MyStreamExt::forward(dummy_stream(), tx.into_sink())
            .map_err(|e| {
                panic!("send error:{:#?}", e)
            });

        let recv_task = rx
            .into_stream()
            .for_each(|item| async move {
                test_msg(item);
            });
        (send_task, recv_task)
        };
        runtime.spawn(send_task);
        runtime.block_on(recv_task);
    });
}

fn smol_channel_buf1_flume(b: &mut Bencher) {
    b.iter(|| {
        let (send_task, recv_task) = {
            let (tx, rx) = flume::bounded::<Message>(1);

            let send_task = MyStreamExt::forward(dummy_stream(), tx.into_sink())
                .map_err(|e| {
                    panic!("send error:{:#?}", e)
                });

            let recv_task = rx
                .into_stream()
                .for_each(|item| async move {
                    test_msg(item);
                });
            (send_task, recv_task)
        };

        smol::spawn(send_task).detach();
        smol::block_on(recv_task);
    });
}

fn execrs_channel_buf1_flume(b: &mut Bencher) {
    use executors::crossbeam_channel_pool::ThreadPool;
    use executors::FuturesExecutor;
    use executors::Executor;
    use futures::executor::block_on;
    let n_workers = 4;
    let pool = ThreadPool::new(n_workers);

    b.iter(|| {
        let (send_task, recv_task) = {
            let (tx, rx) = flume::bounded::<Message>(1);

            let send_task = MyStreamExt::forward(dummy_stream(), tx.into_sink())
                .map_err(|e| {
                    panic!("send error:{:#?}", e)
                });

            let recv_task = rx
                .into_stream()
                .for_each(|item| async move {
                    test_msg(item);
                });
            (send_task, recv_task)
        };

        pool.spawn(send_task).detach();
        let recv = pool.spawn(recv_task);

        block_on(recv);
    });
    pool.shutdown().expect("shutdown");
}

fn execrs_sync_channel_buf1_flume(b: &mut Bencher) {
    use executors::crossbeam_channel_pool::ThreadPool;
    use executors::FuturesExecutor;
    use executors::Executor;
    use futures::executor::block_on;
    let n_workers = 4;
    let pool = ThreadPool::new(n_workers);

    b.iter(|| {
        let (tx, rx) = flume::bounded::<Message>(1);

        let send_task = move || {dummy_iter()
            .for_each(|item| 
                tx.send(item).expect("send error")
            )
        };

        let recv_task = || { rx.iter()
            .for_each(|item| {
                test_msg(item);
            })
        };

        pool.execute(send_task);
        recv_task();
        /*
        let recv = pool.execute(recv_task);

        block_on(recv);
        */
    });
    pool.shutdown().expect("shutdown");
}

fn channel_buf_big_flume(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
        let (send_task, recv_task) = {
        let (tx, rx) = flume::bounded::<Message>(BLOCK_COUNT);

        /*
        let send_task = dummy_stream()
            .fold(tx, |mut tx, item| async move {
                let r = tx.send_async(item).await;
                r.expect("Receiver closed");
                tx
            });
        */
        let send_task = MyStreamExt::forward(dummy_stream(), tx.into_sink())
            .map_err(|e| {
                panic!("send error:{:#?}", e)
            });

        let recv_task = rx
            .into_stream()
            .for_each(|item| async move {
                test_msg(item);
            });
        (send_task, recv_task)
        };
        runtime.spawn(send_task);
        runtime.block_on(recv_task);
    });
}

fn smol_channel_buf_big_flume(b: &mut Bencher) {
    b.iter(|| {
        let (send_task, recv_task) = {
            let (tx, rx) = flume::bounded::<Message>(BLOCK_COUNT);

            let send_task = MyStreamExt::forward(dummy_stream(), tx.into_sink())
                .map_err(|e| {
                    panic!("send error:{:#?}", e)
                });

            let recv_task = rx
                .into_stream()
                .for_each(|item| async move {
                    test_msg(item);
                });
            (send_task, recv_task)
        };

        smol::spawn(send_task).detach();
        smol::block_on(recv_task);
    });
}

fn execrs_channel_buf_big_flume(b: &mut Bencher) {
    use executors::crossbeam_channel_pool::ThreadPool;
    use executors::FuturesExecutor;
    use executors::Executor;
    use futures::executor::block_on;
    let n_workers = 4;
    let pool = ThreadPool::new(n_workers);

    b.iter(|| {
        let (send_task, recv_task) = {
            let (tx, rx) = flume::bounded::<Message>(BLOCK_COUNT);

            let send_task = MyStreamExt::forward(dummy_stream(), tx.into_sink())
                .map_err(|e| {
                    panic!("send error:{:#?}", e)
                });

            let recv_task = rx
                .into_stream()
                .for_each(|item| async move {
                    test_msg(item);
                });
            (send_task, recv_task)
        };

        pool.spawn(send_task).detach();
        let recv = pool.spawn(recv_task);

        block_on(recv);
    });
    pool.shutdown().expect("shutdown");
}

fn channel_buf1_futures(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
        let (send_task, recv_task) = {
        let (tx, rx) = futures::channel::mpsc::channel::<Message>(1);
        let send_task = MyStreamExt::forward(dummy_stream(), tx)
            .map_err(|e| {
                panic!("send error:{:#?}", e)
            });

        let recv_task = rx
            .for_each(|item| async move {
                test_msg(item);
            });
        (send_task, recv_task)
        };
        runtime.spawn(send_task);
        runtime.block_on(recv_task);
    });
}

fn channel_buf_big_futures(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
        let (send_task, recv_task) = {
        let (tx, rx) = futures::channel::mpsc::channel::<Message>(BLOCK_COUNT);
        
        let send_task = MyStreamExt::forward(dummy_stream(), tx)
            .map_err(|e| {
                panic!("send error:{:#?}", e)
            });

        let recv_task = rx
            .for_each(|item| async move {
                test_msg(item);
            });
        (send_task, recv_task)
        };
        runtime.spawn(send_task);
        runtime.block_on(recv_task);
    });
}

fn channel_buf1_futures_fold(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {

        let (send_task, recv_task) = {
        use futures::SinkExt;
        let (tx, rx) = futures::channel::mpsc::channel::<Message>(1);

        let send_task = dummy_stream()
            .fold(tx, |mut tx, item| async move {
                let r = tx.send(item).await;
                r.expect("Receiver closed");
                tx
            });

        let recv_task = rx
            .for_each(|item| async move {
                test_msg(item);
            });
        (send_task, recv_task)
        };
        runtime.spawn(send_task);
        runtime.block_on(recv_task);
    });
}

fn channel_buf_big_futures_fold(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {

        let (send_task, recv_task) = {
        use futures::SinkExt;
        let (tx, rx) = futures::channel::mpsc::channel::<Message>(BLOCK_COUNT);

        let send_task = dummy_stream()
            .fold(tx, |mut tx, item| async move {
                let r = tx.send(item).await;
                r.expect("Receiver closed");
                tx
            });

        let recv_task = rx
            .for_each(|item| async move {
                test_msg(item);
            });
        (send_task, recv_task)
        };
        runtime.spawn(send_task);
        runtime.block_on(recv_task);
    });
}

fn channel_buf1_tokio(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {

        let (send_task, recv_task) = {
        let (tx, rx) = tokio::sync::mpsc::channel::<Message>(1);

        let send_task = dummy_stream()
            .fold(tx, |tx, item| async move {
                let r = tx.send(item).await;
                r.expect("Receiver closed");
                tx
            });

        let recv_task = rx
            .for_each(|item| async move {
                test_msg(item);
            });
        (send_task, recv_task)
        };
        runtime.spawn(send_task);
        runtime.block_on(recv_task);
    });
}

fn channel_buf_big_tokio(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {

        let (send_task, recv_task) = {
        let (tx, rx) = tokio::sync::mpsc::channel::<Message>(BLOCK_COUNT);

        let send_task = dummy_stream()
            .fold(tx, |tx, item| async move {
                let r = tx.send(item).await;
                r.expect("Receiver closed");
                tx
            });

        let recv_task = rx
            .for_each(|item| async move {
                test_msg(item);
            });
        (send_task, recv_task)
        };
        runtime.spawn(send_task);
        runtime.block_on(recv_task);
    });
}

fn channel_buf1_smol(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {

        let (send_task, recv_task) = {
        let (tx, rx) = smol::channel::bounded::<Message>(1);

        let send_task = dummy_stream()
            .fold(tx, |tx, item| async move {
                let r = tx.send(item).await;
                r.expect("Receiver closed");
                tx
            });

        let recv_task = rx
            .for_each(|item| async move {
                test_msg(item);
            });
        (send_task, recv_task)
        };
        runtime.spawn(send_task);
        runtime.block_on(recv_task);
    });
}

fn channel_buf_big_smol(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {

        let (send_task, recv_task) = {
        let (tx, rx) = smol::channel::bounded::<Message>(BLOCK_COUNT);

        let send_task = dummy_stream()
            .fold(tx, |tx, item| async move {
                let r = tx.send(item).await;
                r.expect("Receiver closed");
                tx
            });

        let recv_task = rx
            .for_each(|item| async move {
                test_msg(item);
            });
        (send_task, recv_task)
        };
        runtime.spawn(send_task);
        runtime.block_on(recv_task);
    });
}

fn latency_channel_buf1(_b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    for _i in 0 .. 5 {
        let (tx, rx) = channel::<Instant>(1);

        time_stream().forward_and_spawn(tx, &runtime);

        let recv_task = rx.fold((0u128, 0usize),
        |(sum, len), t| async move {
            let dt = t.elapsed().as_nanos();
            (sum + dt, len +1)
        })
        .map(|(sum, len)| {
            let avg_latency = sum as f64 / len as f64;
            eprintln!("~latency:{:0.1}ns", avg_latency)
        });

        runtime.block_on(recv_task);
    };
}

fn latency_channel_buf_big(_b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    for _i in 0 .. 5 {
        let (tx, rx) = channel::<Instant>(BLOCK_COUNT);

        time_stream().forward_and_spawn(tx, &runtime);

        let recv_task = rx.fold((0u128, 0usize),
        |(sum, len), t| async move {
            let dt = t.elapsed().as_nanos();
            (sum + dt, len +1)
        })
        .map(|(sum, len)| {
            let avg_latency = sum as f64 / len as f64;
            eprintln!("~latency:{:0.1}ns", avg_latency)
        });

        runtime.block_on(recv_task);
    }
}
benchmark_group!(
    channel_latency,
    latency_channel_buf1,
    latency_channel_buf_big,
);

benchmark_group!(
    channel_blocking,
    channel_setup,
    channel_buf1,
    channel_buf1_futures,
    channel_buf1_futures_fold,
    channel_buf1_tokio,
    channel_buf1_smol,
    channel_buf1_flume_fold,
    channel_buf1_flume,
    smol_channel_buf1,
    smol_channel_buf1_flume,
    execrs_channel_buf1,
    execrs_channel_buf1_flume,
    execrs_sync_channel_buf1_flume,
);

benchmark_group!(
    channel_buffered,
    channel_buf_big,
    channel_buf_big_futures,
    channel_buf_big_futures_fold,
    channel_buf_big_tokio,
    channel_buf_big_smol,
    channel_buf_big_flume,
    smol_channel_buf_big,
    smol_channel_buf_big_flume,
    execrs_channel_buf_big,
    execrs_channel_buf_big_flume,
);

benchmark_main!(channel_blocking, channel_buffered, /*channel_latency,*/ );
