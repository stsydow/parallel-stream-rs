use std::time::Instant;
use bytes::Bytes;

use tokio::prelude::*;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::channel;

use test::Bencher;
use test::black_box;

use crate::stream_fork::fork_rr;
use crate::{StreamExt, Tag};

const BLOCK_COUNT:usize = 1_000;

/*
const BLOCK_SIZE:usize = 1<<12; //4k
static PAYLOAD:[u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];
type Message = Bytes;
fn new_message() -> Message {
    Bytes::from_static(&PAYLOAD)
}
*/
type Message = usize;
fn new_message(val:usize) -> Message {
    val
}
fn test_msg(m:Message) {
    black_box(m);
    //let n = m + 2;
    //if m > 0 { () }
}

#[inline(never)]
fn touch(b: Message) {
    test_msg(b);
    //black_box(b)
    //drop(b);
}

#[bench]
fn sync_fn_1k(b: &mut Bencher) {
    b.iter(|| {
        for i in 0.. BLOCK_COUNT {
            let buffer = new_message(i);
            touch(buffer)
        }
    });
}

fn dummy_iter() -> impl Iterator<Item=Message> {
    let mut count = 0;
    let byte_stream = std::iter::from_fn(move || {
        if count < BLOCK_COUNT {
            let buffer = new_message(count);
            count += 1;
            Some(buffer)
        } else {
            None
        }
    });
    byte_stream
}

#[bench]
fn iter_stream_1k(b: &mut Bencher) {
    b.iter(|| {
        dummy_iter()
            .for_each(|item| {
                test_msg(item)
            });
    });
}

//#[inline(never)]
fn dummy_stream() -> impl Stream<Item=Message, Error=()> {
    stream::unfold(0, |count| {
        if count <= BLOCK_COUNT {
            let buffer = new_message(count);
            let fut = future::ok::<_, ()>((buffer, count + 1));
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
            .for_each(|item| {
                test_msg(item);
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

        .for_each(|item| {
            test_msg(item);
            Ok(())
        });
        runtime.block_on(task).expect("error in main task");
    });
}

#[bench]
fn channel_1k(b: &mut Bencher) {
    let mut runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
        let (tx, rx) = channel::<Message>(1);

        let forward = dummy_stream().forward(tx.sink_map_err(|e| panic!("forward send error: {}", e)));

        let src_task = forward.and_then(|(_zero, tx)|  tx.flush())
            .map( |_tx| () )
            .map_err(|_e| ());


        runtime.spawn(src_task);

        let recv_task = rx
            .for_each(|item| {
                test_msg(item);
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
        let (tx, rx) = channel::<Message>(2);

        let forward = dummy_stream().forward(tx.sink_map_err(|e| panic!("forward send error: {}", e)));

        let src_task = forward.and_then(|(_zero, tx)|  tx.flush())
            .map( |_tx| () )
            .map_err(|_e| ());


        runtime.spawn(src_task);

        let recv_task = rx
            .for_each(|item| {
                test_msg(item);
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
        let (tx, rx) = channel::<Vec<Message>>(2);

        let forward = dummy_stream().chunks(10).forward(tx.sink_map_err(|e| panic!("forward send error: {}", e)));

        let src_task = forward.and_then(|(_zero, tx)|  tx.flush())
            .map( |_tx| () )
            .map_err(|_e| ());


        runtime.spawn(src_task);

        let recv_task = rx
            .for_each(|chunk| {
                for item in chunk {
                    test_msg(item)
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
        let (out_tx, out_rx) = channel::<Tag<Message>>(pipe_threads+1);
        for _i in 0..pipe_threads {
            let (in_tx, in_rx) = channel::<Tag<Message>>(2);

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
fn fork_join_tagged_1k(b: &mut Bencher) {
    //let mut runtime = Runtime::new().expect("can not start runtime");
    const pipe_threads:usize = 2;
    b.iter(|| {
        tokio::run(futures::lazy(|| {
            let pipeline = dummy_stream().fork(pipe_threads).decouple(2).join();

            let pipeline_task = pipeline.for_each(|_item| {
                Ok(())
            }).map_err(|_e| ());
            pipeline_task
        }));
        /*
           let pipeline = dummy_stream().fork(2).join();

           let pipeline_task = pipeline.for_each(|_item| {
           Ok(())
           }).map_err(|_e| ());
        //runtime.block_on(pipeline_task).expect("error in main task");
        */
    });
}

#[bench]
fn shuffle_1k(b: &mut Bencher) {
    //let mut runtime = Runtime::new().expect("can not start runtime");
    const pipe_threads:usize = 2;
    b.iter(|| {
        tokio::run(futures::lazy(|| {
            let pipeline = dummy_stream().fork(pipe_threads)
                .shuffle(|i|{i * 7}, pipe_threads).decouple(2).join();

            let pipeline_task = pipeline
            /*
                .fold(0usize, |seq, item| {
                    assert_eq!(seq, item);
                    futures::future::ok(seq +1)
                })
            */
            .for_each(|_item| {
                Ok(())
            })
            .map_err(|_e| ())
            .map(|_val|{()});
            //eprintln!("submit");
            pipeline_task
        }));
        /*
           let pipeline = dummy_stream().fork(2).join();

           let pipeline_task = pipeline.for_each(|_item| {
           Ok(())
           }).map_err(|_e| ());
        //runtime.block_on(pipeline_task).expect("error in main task");
        */
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
        let runtime = Runtime::new().expect("can not start runtime");
        let task = future::lazy(|| future::ok::<(), ()>(()));
        runtime.block_on_all(task).expect("error in main task");
    });
}

#[bench]
fn spawn_1k(b: &mut Bencher) {

    b.iter(|| {
        let mut runtime = Runtime::new().expect("can not start runtime");

        for _i in 0 .. BLOCK_COUNT -1 {
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
fn channel_latency(_b: &mut Bencher) {
    let mut runtime = Runtime::new().expect("can not start runtime");
    for _i in 0 .. 5 {
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
fn channel_buf2_latency(_b: &mut Bencher) {
    let mut runtime = Runtime::new().expect("can not start runtime");
    for _i in 0 .. 5 {
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
fn async_stream_latency(_b: &mut Bencher) {
    let mut runtime = Runtime::new().expect("can not start runtime");
    for _i in 0 .. 5 {
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

#[bench]
fn measure_time(b: &mut Bencher) {
    b.iter( || {
        let t = Instant::now();
        //let dt = t.elapsed()//.as_nanos();
        //dt
        t
    });
}
