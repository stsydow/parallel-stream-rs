use std::time::Instant;
use bytes::Bytes;

use tokio::prelude::*;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::channel;

use test::Bencher;
//use test::black_box;

use libc::{c_long, rusage, suseconds_t, timeval, time_t, getrusage, RUSAGE_SELF};

use crate::stream_fork::fork_rr;
use crate::stream_join::Join;

pub fn get_cputime_usecs() -> (u64, u64) {
    let mut usage = rusage {
        ru_utime: timeval{ tv_sec: 0 as time_t, tv_usec: 0 as suseconds_t, },
        ru_stime: timeval{ tv_sec: 0 as time_t, tv_usec: 0 as suseconds_t, },
        ru_maxrss: 0 as c_long,
        ru_ixrss: 0 as c_long,
        ru_idrss: 0 as c_long,
        ru_isrss: 0 as c_long,
        ru_minflt: 0 as c_long,
        ru_majflt: 0 as c_long,
        ru_nswap: 0 as c_long,
        ru_inblock: 0 as c_long,
        ru_oublock: 0 as c_long,
        ru_msgsnd: 0 as c_long,
        ru_msgrcv: 0 as c_long,
        ru_nsignals: 0 as c_long,
        ru_nvcsw: 0 as c_long,
        ru_nivcsw: 0 as c_long,
    };

    unsafe { getrusage(RUSAGE_SELF, (&mut usage) as *mut rusage); }

    let u_secs = usage.ru_utime.tv_sec as u64;
    let u_usecs = usage.ru_utime.tv_usec as u64;
    let s_secs = usage.ru_stime.tv_sec as u64;
    let s_usecs = usage.ru_stime.tv_usec as u64;

    let u_time = (u_secs * 1_000_000) + u_usecs;
    let s_time = (s_secs * 1_000_000) + s_usecs;

    (u_time, s_time)
}

const BLOCK_COUNT:usize = 1_000;
const BLOCK_SIZE:usize = 1<<12; //4k
static PAYLOAD:[u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];


#[inline(never)]
fn touch(b: Bytes) {
    //black_box(b)

    if b[0] > 0 {
        panic!("unexpected value")
    }
    //drop(b);
}

#[bench]
fn sync_fn_1k(b: &mut Bencher) {
    b.iter(|| {
        for _i in 0.. BLOCK_COUNT {
            let buffer = Bytes::from_static(&PAYLOAD);
            touch(buffer)
        }
    });
}

//#[inline(never)]
fn dummy_stream() -> impl Stream<Item=Bytes, Error=()> {
    stream::unfold(0, |count| {
        if count <= BLOCK_COUNT {
            let count = count + 1;
            let buffer = Bytes::from_static(&PAYLOAD);
            let fut = future::ok::<_, ()>((buffer, count));
            Some(fut)
        } else {
            None
        }
    })
}

#[bench]
fn async_stream_1k(b: &mut Bencher) {
    let mut runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
        let task = dummy_stream()
            .for_each(|_item| {
                //touch(item);
                Ok(())
            });
        runtime.block_on(task).expect("error in main task");
    });
}

#[bench]
fn async_stream_map10_1k(b: &mut Bencher) {
    let mut runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
    let task = dummy_stream()
        .map(|i| i)
        .map(|i| i)
        .map(|i| i)
        .map(|i| i)
        .map(|i| i)

        .map(|i| i)
        .map(|i| i)
        .map(|i| i)
        .map(|i| i)
        .map(|i| i)

        .for_each(|_item| {
            Ok(())
        });
        runtime.block_on(task).expect("error in main task");
    });
}

#[bench]
fn channel_1k(b: &mut Bencher) {
    let mut runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
        let (tx, rx) = channel::<Bytes>(1);

        let forward = dummy_stream().forward(tx.sink_map_err(|e| panic!("forward send error: {}", e)));

        let src_task = forward.and_then(|(_zero, tx)|  tx.flush())
            .map( |_tx| () )
            .map_err(|_e| ());


        runtime.spawn(src_task);

        let recv_task = rx
            .for_each(|_item| {
                Ok(())
            })
        .map_err(|_e| ());


        runtime.block_on(recv_task).expect("error in main task");
    });
}

#[bench]
fn channel_buf2_1k(b: &mut Bencher) {
    let mut runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
        let (tx, rx) = channel::<Bytes>(2);

        let forward = dummy_stream().forward(tx.sink_map_err(|e| panic!("forward send error: {}", e)));

        let src_task = forward.and_then(|(_zero, tx)|  tx.flush())
            .map( |_tx| () )
            .map_err(|_e| ());


        runtime.spawn(src_task);

        let recv_task = rx
            .for_each(|_item| {
                Ok(())
            })
        .map_err(|_e| ());


        runtime.block_on(recv_task).expect("error in main task");
    });
}

#[bench]
fn channel_buf2_chunk10_1k(b: &mut Bencher) {
    let mut runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
        let (tx, rx) = channel::<Vec<Bytes>>(2);

        let forward = dummy_stream().chunks(10).forward(tx.sink_map_err(|e| panic!("forward send error: {}", e)));

        let src_task = forward.and_then(|(_zero, tx)|  tx.flush())
            .map( |_tx| () )
            .map_err(|_e| ());


        runtime.spawn(src_task);

        let recv_task = rx
            .for_each(|chunk| {
                for item in chunk {
                    if item.len() == 0 {
                        panic!("expected a full buffer");
                    }
                }
                Ok(())
            })
        .map_err(|_e| ());

        runtime.block_on(recv_task).expect("error in main task");
    });
}

#[bench]
fn fork_join_1k(b: &mut Bencher) {
    let mut runtime = Runtime::new().expect("can not start runtime");
    let pipe_threads:usize = 2;
    b.iter(|| {

    let (fork, join) = {
        let mut senders = Vec::new();
        //let mut join = Join::new();
        let (out_tx, out_rx) = channel::<Bytes>(pipe_threads+1);
        for _i in 0..pipe_threads {
            let (in_tx, in_rx) = channel::<Bytes>(2);

            senders.push(in_tx);
            let forward = in_rx.forward(out_tx.clone().sink_map_err(|e| panic!("forward send error: {}", e)));

            let pipe_task = forward.and_then(|(_zero, tx)|  tx.flush())
                .map( |_tx| () )
                .map_err(|_e| ());
            runtime.spawn(pipe_task);
        }

        let fork = fork_rr(senders);

        (fork, out_rx)
    };

    let forward = dummy_stream().forward(fork.sink_map_err(|e| panic!("forward send error: {}", e)));

    let src_task = forward.and_then(|(_zero, tx)|  tx.flush())
        .map( |_tx| () )
        .map_err(|_e| ());


    runtime.spawn(src_task);

    let recv_task = join
        .for_each(|_item| {
            Ok(())
        })
        .map_err(|_e| ());


        runtime.block_on(recv_task).expect("error in main task");
    });
}

#[bench]
fn wait_task(b: &mut Bencher) {
    let mut runtime = Runtime::new().expect("can not start runtime");

    b.iter(|| {

        let task = future::lazy(|| future::ok::<(), std::io::Error>(()) );

        //runtime.spawn(task);

        runtime.block_on(task).expect("error in main task");
    });
}

#[bench]
fn spawn_1(b: &mut Bencher) {

    b.iter(|| {
        let mut runtime = Runtime::new().expect("can not start runtime");
        let task = future::lazy(|| future::ok::<(), ()>(()));
        runtime.block_on_all(task).expect("error in main task");
    });
}

#[bench]
fn spawn_1k(b: &mut Bencher) {

    b.iter(|| {
        let mut runtime = Runtime::new().expect("can not start runtime");

        for _i in 0 .. BLOCK_SIZE -1 {
            let task = future::lazy(|| future::ok::<(), ()>(()) );

            runtime.spawn(task);
        }

        let task = future::lazy(|| future::ok::<(), ()>(()));
        runtime.block_on_all(task).expect("error in main task");
    });
}


fn time_stream() -> impl Stream<Item=Instant, Error=()> {
    stream::unfold(0, |count| {
        if count <= BLOCK_COUNT {
            let count = count + 1;
            let t = Instant::now();
            let fut = future::ok::<_, ()>((t, count));
            Some(fut)
        } else {
            None
        }
    })
}

#[bench]
fn channel_latency(b: &mut Bencher) {
    let mut runtime = Runtime::new().expect("can not start runtime");
    for i in 0 .. 5 {
        let (tx, rx) = channel::<Instant>(1);

        let forward = time_stream().forward(tx.sink_map_err(|e| panic!("forward send error: {}", e)));

        let src_task = forward.and_then(|(_zero, tx)|  tx.flush())
            .map( |_tx| () )
            .map_err(|_e| ());


        runtime.spawn(src_task);

        let recv_task = rx.fold((0u128, 0usize),
        |(sum, len), t| {
            let dt = t.elapsed().as_nanos();
            future::ok((sum + dt, len +1))
        })
        .map(|(sum, len)| {
            let avg_latency = sum as f64 / len as f64;
            eprintln!("~latency:{:0.1}ns", avg_latency)
        })
        .map_err(|_e| ());


        runtime.block_on(recv_task).expect("error in main task");
    };
}

#[bench]
fn channel_buf2_latency(b: &mut Bencher) {
    let mut runtime = Runtime::new().expect("can not start runtime");
    for i in 0 .. 5 {
        let (tx, rx) = channel::<Instant>(2);

        let forward = time_stream().forward(tx.sink_map_err(|e| panic!("forward send error: {}", e)));

        let src_task = forward.and_then(|(_zero, tx)|  tx.flush())
            .map( |_tx| () )
            .map_err(|_e| ());


        runtime.spawn(src_task);

        let recv_task = rx.fold((0u128, 0usize),
        |(sum, len), t| {
            let dt = t.elapsed().as_nanos();
            future::ok((sum + dt, len +1))
        })
        .map(|(sum, len)| {
            let avg_latency = sum as f64 / len as f64;
            eprintln!("~latency:{:0.1}ns", avg_latency)
        })
        .map_err(|_e| ());


        runtime.block_on(recv_task).expect("error in main task");
    }
}

#[bench]
fn async_stream_latency(b: &mut Bencher) {
    let mut runtime = Runtime::new().expect("can not start runtime");
    for i in 0 .. 5 {
        let task = time_stream().fold((0u128, 0usize),
        |(sum, len), t| {
            let dt = t.elapsed().as_nanos();
            future::ok((sum + dt, len +1))
        })
        .map(|(sum, len)| {
            let avg_latency = sum as f64 / len as f64;
            eprintln!("~latency:{:0.1}ns", avg_latency)
        })
        .map_err(|_e| ());

        runtime.block_on(task).expect("error in main task");
    }
}
