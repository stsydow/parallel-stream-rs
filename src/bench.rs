use std::time::Instant;

use tokio::prelude::*;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::channel;

use bencher::{benchmark_group, benchmark_main, black_box, Bencher};

use parallel_stream::{fork_rr, StreamExt};

const BLOCK_COUNT: usize = 1_000;

const THREADS: usize = 8;
/*
//use bytes::Bytes;
const BLOCK_SIZE:usize = 1<<12; //4k
static PAYLOAD:[u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];
type Message = Bytes;
fn new_message() -> Message {
    Bytes::from_static(&PAYLOAD)
}
*/

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new()
        .core_threads(THREADS)
        .build()
        .unwrap()
}

type Message = usize;
fn new_message(val: usize) -> Message {
    val
}
fn test_msg(m: Message) {
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

//#[bench]
fn sync_fn(b: &mut Bencher) {
    b.iter(|| {
        for i in 0..BLOCK_COUNT {
            let buffer = new_message(i);
            touch(buffer)
        }
    });
}

fn dummy_iter() -> impl Iterator<Item = Message> {
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

//#[bench]
fn iter_stream(b: &mut Bencher) {
    b.iter(|| {
        dummy_iter().for_each(|item| test_msg(item));
    });
}

//#[inline(never)]
fn dummy_stream() -> impl Stream<Item = Message, Error = ()> {
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

//#[bench]
fn async_stream(b: &mut Bencher) {
    let mut runtime = rt();
    b.iter(|| {
        let task = dummy_stream().for_each(|item| {
            test_msg(item);
            Ok(())
        });
        runtime.block_on(task).expect("error in main task");
    });
}

//#[bench]
fn async_stream_map10(b: &mut Bencher) {
    let mut runtime = rt();
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

//#[bench]
fn selective_context(b: &mut Bencher) {
    let mut runtime = rt();
    b.iter(|| {
        let task = dummy_stream()
            .selective_context(
                |_| 0,
                |i| *i,
                |cx, i| {
                    *cx += i;
                    i
                },
                "sel_test".to_owned(),
            )
            .for_each(|item| {
                test_msg(item);
                Ok(())
            });
        runtime.block_on(task).expect("error in main task");
    });
}

//#[bench]
fn selective_context_buf(b: &mut Bencher) {
    use parallel_stream::StreamChunkedExt;
    let mut runtime = rt();
    b.iter(|| {
        let task = dummy_stream()
            .chunks(100)
            .selective_context_buffered(
                |_| 0,
                |i| *i,
                |cx, i| {
                    *cx += i;
                    i
                },
                "sel_test".to_owned(),
            )
            .for_each(|chunk| {
                for item in chunk {
                    test_msg(item);
                }
                Ok(())
            });
        runtime.block_on(task).expect("error in main task");
    });
}

benchmark_group!(
    async_fn,
    sync_fn,
    iter_stream,
    async_stream,
    async_stream_map10,
    selective_context_buf,
    selective_context
);

//#[bench]
fn channel_buf1(b: &mut Bencher) {
    let mut runtime = rt();
    b.iter(|| {
        let (tx, rx) = channel::<Message>(1);

        dummy_stream().forward_and_spawn(tx, &mut runtime.executor());
        let recv_task = rx
            .for_each(|item| {
                test_msg(item);
                Ok(())
            })
            .map_err(|_e| ());

        runtime.block_on(recv_task).expect("error in main task");
    });
}

//#[bench]
fn channel_buf_big(b: &mut Bencher) {
    let mut runtime = rt();
    b.iter(|| {
        let (tx, rx) = channel::<Message>(BLOCK_COUNT);

        dummy_stream().forward_and_spawn(tx, &mut runtime.executor());

        let recv_task = rx
            .for_each(|item| {
                test_msg(item);
                Ok(())
            })
            .map_err(|_e| ());

        runtime.block_on(recv_task).expect("error in main task");
    });
}

//#[bench]
fn channel_buf_big_chunk10(b: &mut Bencher) {
    let mut runtime = rt();
    b.iter(|| {
        let (tx, rx) = channel::<Vec<Message>>(BLOCK_COUNT / 10);

        dummy_stream()
            .chunks(10)
            .forward_and_spawn(tx, &mut runtime.executor());

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
benchmark_group!(
    mpsc_channel,
    channel_buf1,
    channel_buf_big,
    channel_buf_big_chunk10
);

//#[bench]
fn fork_join(b: &mut Bencher) {
    let mut runtime = rt();
    let mut exec = runtime.executor();
    b.iter(|| {
        let (fork, join) = {
            let mut senders = Vec::new();
            //let mut join = Join::new();
            let (out_tx, out_rx) = channel::<Message>(THREADS * 10);
            for _i in 0..THREADS {
                let (in_tx, in_rx) = channel::<Message>(BLOCK_COUNT / 10);

                senders.push(in_tx);
                in_rx.forward_and_spawn(out_tx.clone(), &mut exec);
            }

            let fork = fork_rr(senders);

            (fork, out_rx)
        };

        dummy_stream().forward_and_spawn(fork, &mut exec);

        let recv_task = join.for_each(|_item| Ok(())).map_err(|_e| ());

        runtime.block_on(recv_task).expect("error in main task");
    });
}

//#[bench]
fn fork_join_unorderd(b: &mut Bencher) {
    let mut runtime = rt();
    let mut exec = runtime.executor();
    b.iter(|| {
        let pipeline = dummy_stream()
            .fork(THREADS, BLOCK_COUNT / 10, &mut exec)
            .join_unordered(THREADS * 10, &mut exec);

        let pipeline_task = pipeline.for_each(|_item| Ok(())).map_err(|_e| ());

        runtime.block_on(pipeline_task).expect("error in main task");
    });
}

//#[bench]
fn shuffle(b: &mut Bencher) {
    let mut runtime = rt();
    let mut exec = runtime.executor();
    b.iter(|| {
        let pipeline = dummy_stream()
            .fork(THREADS, BLOCK_COUNT / 10, &mut exec)
            .shuffle_unordered(|i| i * 7, THREADS, BLOCK_COUNT / 10, &mut exec)
            .join_unordered(10 * THREADS, &mut exec);

        let pipeline_task = pipeline.for_each(|_item| Ok(())).map_err(|_e| ());
        runtime.block_on(pipeline_task).expect("error in main task");
    });
}

benchmark_group!(parallel, fork_join, fork_join_unorderd, shuffle);
/*
use std::collections::HashMap;
pub type FreqTable = HashMap<Bytes, u64>;

//#[inline(never)]
pub fn count_bytes(frequency: &mut FreqTable, text: &Bytes) -> usize {
    let mut i_start: usize = 0;
    for i in 0..text.len() {
        if text[i].is_ascii_whitespace() {
            let word = text.slice(i_start, i);
            if !word.is_empty() {
                *frequency.entry(word).or_insert(0) += 1;
            }
            i_start = i + 1;
        }
    }

    i_start
}

#[bench]
fn file_io(b: &mut Bencher) {
    use std::iter::FromIterator;
    use crate::stream_ext::StreamChunkedExt;
    use tokio::executor::DefaultExecutor;
    use tokio::sync::mpsc::Receiver;
    b.iter(||
    {
        use tokio::fs::{File, OpenOptions};
        use tokio::codec::{BytesCodec, FramedRead, FramedWrite};
        let filename = "~/dev/test_data/10M_rand_text.txt";
        let file_future = File::open(filename);

        tokio::run(file_future.map(|file| {
            let mut exec = DefaultExecutor::current();
            let input_stream = FramedRead::new(file, BytesCodec::new());
            let sub_table_streams: Receiver<Vec<(Bytes, u64)>> = input_stream
                .fork(THREADS, 2, &mut exec)
                .instrumented_fold(|| FreqTable::new(), |mut frequency, text| {
                    let text = text.freeze();
                    count_bytes(&mut frequency, &text);

                    future::ok::<FreqTable, _>(frequency)
                }, "split_and_count".to_owned())
                .map_result(|frequency| Vec::from_iter(frequency))
                ... stream::iter_ok(frequency) ...
                //.map_err(|e| {panic!(); ()})
                .fork_sel_chunked( |(word, _count)| word.len() , THREADS, 2, &mut exec )
                .instrumented_fold(|| FreqTable::new(), |mut frequency, chunk| {

                    for (word, count) in chunk {
                        *frequency.entry(word).or_insert(0) += count;
                    }

                    future::ok::<FreqTable, _>(frequency)
                }, "merge_table".to_owned()).map_result(|sub_table| {
                    Vec::from_iter(sub_table)
                } )
                //.merge( init, f, &mut exec)
                ;


            // unimplemented!();
                ()
            // TODO src/word_count_para_partition_shuffle_chunked.rs
        }).map_err(|_e| ()) );
    });

}
*/

//#[bench]
fn wait_task(b: &mut Bencher) {
    let mut runtime = rt();

    b.iter(|| {
        let task = future::lazy(|| future::ok::<(), std::io::Error>(()));

        runtime.block_on(task).expect("error in main task");
    });
}

//#[bench]
fn spawn_1(b: &mut Bencher) {
    b.iter(|| {
        let runtime = rt();
        let task = future::lazy(|| future::ok::<(), ()>(()));
        runtime.block_on_all(task).expect("error in main task");
    });
}

//#[bench]
fn spawn(b: &mut Bencher) {
    b.iter(|| {
        let mut runtime = rt();

        for _i in 0..BLOCK_COUNT - 1 {
            let task = future::lazy(|| future::ok::<(), ()>(()));

            runtime.spawn(task);
        }

        let task = future::lazy(|| future::ok::<(), ()>(()));
        runtime.block_on_all(task).expect("error in main task");
    });
}
benchmark_group!(task_sched, spawn, spawn_1, wait_task);

fn time_stream() -> impl Stream<Item = Instant, Error = ()> {
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

//#[bench]
fn channel_buf1_latency(_b: &mut Bencher) {
    let mut runtime = rt();
    for _i in 0..5 {
        let (tx, rx) = channel::<Instant>(1);

        time_stream().forward_and_spawn(tx, &mut runtime.executor());

        let recv_task = rx
            .fold((0u128, 0usize), |(sum, len), t| {
                let dt = t.elapsed().as_nanos();
                future::ok((sum + dt, len + 1))
            })
            .map(|(sum, len)| {
                let avg_latency = sum as f64 / len as f64;
                eprintln!("~latency:{:0.1}ns", avg_latency)
            })
            .map_err(|_e| ());

        runtime.block_on(recv_task).expect("error in main task");
    }
}

//#[bench]
fn channel_buf_big_latency(_b: &mut Bencher) {
    let mut runtime = rt();
    for _i in 0..5 {
        let (tx, rx) = channel::<Instant>(BLOCK_COUNT);

        time_stream().forward_and_spawn(tx, &mut runtime.executor());

        let recv_task = rx
            .fold((0u128, 0usize), |(sum, len), t| {
                let dt = t.elapsed().as_nanos();
                future::ok((sum + dt, len + 1))
            })
            .map(|(sum, len)| {
                let avg_latency = sum as f64 / len as f64;
                eprintln!("~latency:{:0.1}ns", avg_latency)
            })
            .map_err(|_e| ());

        runtime.block_on(recv_task).expect("error in main task");
    }
}

//#[bench]
fn async_stream_latency(_b: &mut Bencher) {
    let mut runtime = rt();
    for _i in 0..5 {
        let task = time_stream()
            .fold((0u128, 0usize), |(sum, len), t| {
                let dt = t.elapsed().as_nanos();
                future::ok((sum + dt, len + 1))
            })
            .map(|(sum, len)| {
                let avg_latency = sum as f64 / len as f64;
                eprintln!("~latency:{:0.1}ns", avg_latency)
            })
            .map_err(|_e| ());

        runtime.block_on(task).expect("error in main task");
    }
}
benchmark_group!(
    latency,
    async_stream_latency,
    channel_buf_big_latency,
    channel_buf1_latency,
);

//#[bench]
fn measure_time(b: &mut Bencher) {
    b.iter(|| {
        let t = Instant::now();
        //let dt = t.elapsed()//.as_nanos();
        //dt
        t
    });
}
benchmark_group!(time, measure_time);

use std::fs::File as StdFile;
use std::io::Read as StdRead;
use tokio::codec::{BytesCodec, FramedRead /*FramedWrite*/};
use tokio::fs::File;

const BUFFER_SIZE: usize = 4096;
const DEV_ZERO: &'static str = "/dev/zero";

fn async_read_codec(b: &mut Bencher) {
    let mut runtime = rt();

    b.iter(|| {
        let task = File::open(DEV_ZERO).and_then(|file| {
            let input_stream = FramedRead::new(file, BytesCodec::new());
            input_stream.take(BLOCK_COUNT as u64).for_each(|_| Ok(()))
        });
        runtime.block_on(task).expect("task error");
    });
}

fn async_read(b: &mut Bencher) {
    let mut runtime = rt();

    b.iter(|| {
        let task = File::open(DEV_ZERO).and_then(move |mut file| {
            let mut buffer = [0u8; BUFFER_SIZE];
            stream::poll_fn(move || {
                let r = match file.poll_read(&mut buffer)? {
                    Async::Ready(count) => Async::Ready(Some(count)),
                    Async::NotReady => Async::NotReady,
                };
                Ok(r)
            })
            .take(BLOCK_COUNT as u64)
            .for_each(|_| Ok(()))
        });

        runtime.block_on(task).expect("task error");
    });
}

fn sync_read(b: &mut Bencher) {
    b.iter(|| {
        let mut file = StdFile::open(DEV_ZERO).unwrap();
        let mut buffer = [0u8; BUFFER_SIZE];

        for _i in 0..BLOCK_COUNT {
            file.read_exact(&mut buffer).unwrap();
        }
    });
}

benchmark_group!(file, async_read, async_read_codec, sync_read);

benchmark_main!(
    mpsc_channel,
    async_fn,
    time,
    /*latency,*/ parallel,
    task_sched,
    file
);
