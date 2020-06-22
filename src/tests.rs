use std::time::Instant;
use bytes::Bytes;

use tokio::prelude::*;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{Receiver, channel};
use tokio::executor::DefaultExecutor;

use test::Bencher;
use test::black_box;

use crate::stream_fork::fork_rr;
use crate::{StreamExt};

const BLOCK_COUNT:usize = 1_000;

const THREADS:usize = 8;
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
fn sync_fn(b: &mut Bencher) {
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
fn iter_stream(b: &mut Bencher) {
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
fn async_stream(b: &mut Bencher) {
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
fn async_stream_map10(b: &mut Bencher) {
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
fn channel_buf1(b: &mut Bencher) {
    let mut runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
        let (tx, rx) = channel::<Message>(1);

        dummy_stream().forward_and_spawn(tx,&mut runtime.executor());
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
fn channel_buf2(b: &mut Bencher) {
    let mut runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
        let (tx, rx) = channel::<Message>(2);

        dummy_stream().forward_and_spawn(tx,&mut runtime.executor());

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
fn channel_buf2_chunk10(b: &mut Bencher) {
    let mut runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
        let (tx, rx) = channel::<Vec<Message>>(2);

        dummy_stream().chunks(10).forward_and_spawn(tx,&mut runtime.executor());

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
fn fork_join(b: &mut Bencher) {
    let mut runtime = Runtime::new().expect("can not start runtime");
    let mut exec = runtime.executor();
    b.iter(|| {

    let (fork, join) = {
        let mut senders = Vec::new();
        //let mut join = Join::new();
        let (out_tx, out_rx) = channel::<Message>(THREADS + 1);
        for _i in 0 .. THREADS {
            let (in_tx, in_rx) = channel::<Message>(2);

            senders.push(in_tx);
            in_rx.forward_and_spawn(out_tx.clone(), &mut exec);
        }

        let fork = fork_rr(senders);

        (fork, out_rx)
    };

    dummy_stream().forward_and_spawn(fork, &mut exec);

    let recv_task = join
        .for_each(|_item| {
            Ok(())
        })
        .map_err(|_e| ());


        runtime.block_on(recv_task).expect("error in main task");
    });
}

#[bench]
fn fork_join_unorderd(b: &mut Bencher) {
    let mut runtime = Runtime::new().expect("can not start runtime");
    let mut exec = runtime.executor();
    b.iter(|| {
            let pipeline = dummy_stream()
                .fork(THREADS, 2, &mut exec)
                .join_unordered(THREADS +1, &mut exec);

            let pipeline_task = pipeline
            .for_each(|_item| {
                Ok(())
            })
            .map_err(|_e| ());
            
            runtime.block_on(pipeline_task).expect("error in main task");
    });
}

#[bench]
fn shuffle(b: &mut Bencher) {
    let mut runtime = Runtime::new().expect("can not start runtime");
    let mut exec = runtime.executor();
    b.iter(|| {
            let pipeline = dummy_stream()
                .fork(THREADS, 2, &mut exec)
                .shuffle_unordered(|i|{i * 7}, THREADS, 2, &mut exec)
                .join_unordered(2*THREADS, &mut exec);

            let pipeline_task = pipeline
            .for_each(|_item| {
                Ok(())
            })
            .map_err(|_e| ());
            runtime.block_on(pipeline_task).expect("error in main task");
        });
}

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

#[bench]
fn wait_task(b: &mut Bencher) {
    let mut runtime = Runtime::new().expect("can not start runtime");

    b.iter(|| {

        let task = future::lazy(|| future::ok::<(), std::io::Error>(()) );

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
fn spawn(b: &mut Bencher) {

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
fn channel_buf1_latency(_b: &mut Bencher) {
    let mut runtime = Runtime::new().expect("can not start runtime");
    for _i in 0 .. 5 {
        let (tx, rx) = channel::<Instant>(1);

        time_stream().forward_and_spawn(tx, &mut runtime.executor());

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

        time_stream().forward_and_spawn(tx, &mut runtime.executor());

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
