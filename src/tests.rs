use std::time::Instant;
//use bytes::Bytes;

use tokio::runtime::Runtime;
use futures::future::{self, FutureExt};
use futures::stream::{self, Stream, StreamExt};

use test::Bencher;
use test::black_box;

//use crate::Receiver;
use crate::channel;
use crate::stream_fork::fork_rr;
use crate::stream_ext::StreamExt as MyStreamExt;

const BLOCK_COUNT:usize = 1_000;

const THREADS:usize = 8;

const CHANNEL_BUFFER:usize = 100;
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
fn dummy_stream() -> impl Stream<Item=Message> {
    stream::unfold(0, |count| async move {
        if count < BLOCK_COUNT {
            let buffer = new_message(count);
            Some((buffer, count + 1))
        } else {
            None
        }
    })
}

#[bench]
fn async_stream(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
        let task = dummy_stream()
            .for_each(|item| async move {
                test_msg(item);
            });
        runtime.block_on(task);
    });
}

#[bench]
fn async_stream_map10(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
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

        .for_each(|item| async move {
            test_msg(item);
        });
        runtime.block_on(task);
    });
}

#[bench]
fn channel_setup(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
        let (tx, rx) = channel::<Message>(1);

        futures::stream::once(async {new_message(1)})
        .forward_and_spawn(tx, &runtime);
        let recv_task = rx
            .for_each(|item| async move {
                test_msg(item);
            });


        runtime.block_on(recv_task);
    });
}

#[bench]
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

#[bench]
fn channel_buf2(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
        let (tx, rx) = channel::<Message>(2);

        dummy_stream().forward_and_spawn(tx,&runtime);

        let recv_task = rx
            .for_each(|item| async move {
                test_msg(item);
            });

        runtime.block_on(recv_task);
    });
}

#[bench]
fn channel_buf10(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
        let (tx, rx) = channel::<Message>(10);

        dummy_stream().forward_and_spawn(tx,&runtime);

        let recv_task = rx
            .for_each(|item| async move {
                test_msg(item);
            });

        runtime.block_on(recv_task);
    });
}

#[bench]
fn channel_buf100(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
        let (tx, rx) = channel::<Message>(100);

        dummy_stream().forward_and_spawn(tx,&runtime);

        let recv_task = rx
            .for_each(|item| async move {
                test_msg(item);
            });

        runtime.block_on(recv_task);
    });
}

#[bench]
fn channel_buf1_flume(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
        let (send_task, recv_task) = {
        use futures::SinkExt;
        let (tx, rx) = flume::bounded::<Message>(1);

        let send_task = dummy_stream()
            .fold(tx, |mut tx, item| async move {
                let r = tx.send_async(item).await;
                r.expect("Receiver closed");
                tx
            });
        /*
        let send_task = dummy_stream()
            .map(|i| Ok(i))
            .forward(tx.into_sink().sink_map_err(|e| {
                panic!("send error:{:#?}", e)
            }));
        */

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

#[bench]
fn channel_buf100_flume(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
        let (send_task, recv_task) = {
        use futures::SinkExt;
        let (tx, rx) = flume::bounded::<Message>(100);

        let send_task = dummy_stream()
            .fold(tx, |mut tx, item| async move {
                let r = tx.send_async(item).await;
                r.expect("Receiver closed");
                tx
            });
        /*
        let send_task = dummy_stream()
            .map(|i| Ok(i))
            .forward(tx.into_sink().sink_map_err(|e| {
                panic!("send error:{:#?}", e)
            }));
        */

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

#[bench]
fn channel_buf1_futures(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
        let (send_task, recv_task) = {
        use futures::SinkExt;
        let (tx, rx) = futures::channel::mpsc::channel::<Message>(1);
        let send_task = dummy_stream()
            .map(|i| Ok(i))
            .forward(tx.sink_map_err(|e| {
                panic!("send error:{:#?}", e)
            }));

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

#[bench]
fn channel_buf100_futures(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
        let (send_task, recv_task) = {
        use futures::SinkExt;
        let (tx, rx) = futures::channel::mpsc::channel::<Message>(100);
        
        let send_task = dummy_stream()
            .map(|i| Ok(i))
            .forward(tx.sink_map_err(|e| {
                panic!("send error:{:#?}", e)
            }));

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

#[bench]
fn channel_buf100_futures_fold(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {

        let (send_task, recv_task) = {
        use futures::SinkExt;
        let (tx, rx) = futures::channel::mpsc::channel::<Message>(100);

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

#[bench]
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

#[bench]
fn channel_buf2_tokio(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {

        let (send_task, recv_task) = {
        let (tx, rx) = tokio::sync::mpsc::channel::<Message>(2);

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

#[bench]
fn channel_buf10_tokio(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {

        let (send_task, recv_task) = {
        let (tx, rx) = tokio::sync::mpsc::channel::<Message>(10);

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
#[bench]
fn channel_buf100_tokio(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {

        let (send_task, recv_task) = {
        let (tx, rx) = tokio::sync::mpsc::channel::<Message>(100);

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


#[bench]
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

#[bench]
fn channel_buf2_smol(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {

        let (send_task, recv_task) = {
        let (tx, rx) = smol::channel::bounded::<Message>(2);

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

#[bench]
fn channel_buf10_smol(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {

        let (send_task, recv_task) = {
        let (tx, rx) = smol::channel::bounded::<Message>(10);

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

#[bench]
fn channel_buf100_smol(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {

        let (send_task, recv_task) = {
        let (tx, rx) = smol::channel::bounded::<Message>(100);

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

#[bench]
fn channel_buf10_chunk10(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
        let (tx, rx) = channel::<Vec<Message>>(10);

        dummy_stream().chunks(10).forward_and_spawn(tx,&runtime);

        let recv_task = rx
            .for_each(|chunk| async {
                for item in chunk {
                    test_msg(item);
                }
            });

        runtime.block_on(recv_task);
    });
}

#[bench]
fn fork_join(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    let exec = Box::pin(runtime);
    b.iter(|| {

    let (fork, join) = {
        let mut senders = Vec::new();
        //let mut join = Join::new();
        let (out_tx, out_rx) = channel::<Message>(THREADS*CHANNEL_BUFFER);
        for _i in 0 .. THREADS {
            let (in_tx, in_rx) = channel::<Message>(CHANNEL_BUFFER);

            senders.push(in_tx);
            in_rx.forward_and_spawn(out_tx.clone(), &exec);
        }

        let fork = fork_rr(senders);

        (fork, out_rx)
    };

    dummy_stream().forward_and_spawn(fork, &exec);

    let recv_task = join
        .for_each(|_item| async {()});

        exec.block_on(recv_task);
    });
}

#[bench]
fn fork_join_unorderd(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    let exec = &runtime;
    b.iter(|| {
            let pipeline = dummy_stream()
                .fork(THREADS, CHANNEL_BUFFER, &exec)
                .join_unordered(CHANNEL_BUFFER*THREADS, &exec);

            let pipeline_task = pipeline
            .for_each(|_item| async {()});
            
            exec.block_on(pipeline_task);
    });
}

#[bench]
fn shuffle_1(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    let exec = Box::pin(runtime);
    b.iter(|| {
            let pipeline = dummy_stream()
                .fork(THREADS, CHANNEL_BUFFER, &exec)
                .shuffle_unordered(|i|{*i}, THREADS, CHANNEL_BUFFER, &exec)
                .join_unordered(CHANNEL_BUFFER*THREADS, &exec);

            let pipeline_task = pipeline
            .for_each(|_item| async {
                ()
            });
            
            exec.block_on(pipeline_task);
        });
}

#[bench]
fn shuffle_unfair(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    let exec = Box::pin(runtime);
    b.iter(|| {
            let pipeline = dummy_stream()
                .fork(THREADS, CHANNEL_BUFFER, &exec)
                .shuffle_unordered(|_i|{0}, THREADS, CHANNEL_BUFFER, &exec)
                .join_unordered(CHANNEL_BUFFER*THREADS, &exec);

            let pipeline_task = pipeline
            .for_each(|_item| async {
                ()
            });
            
            exec.block_on(pipeline_task);
        });
}

#[bench]
fn shuffle_nobuffer(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    let exec = Box::pin(runtime);
    b.iter(|| {
            let pipeline = dummy_stream()
                .fork(THREADS, 1, &exec)
                /*.map(|i| {
                    eprintln!("f{}", i);
                    i
                })*/
                .shuffle_unordered(|_i|{0}, THREADS, 1, &exec)
                /*.map(|i| {
                    eprintln!("s{}", i);
                    i
                })*/
                .join_unordered(1, &exec);

            let pipeline_task = pipeline
            .for_each(|_item| async {
                ()
            });
            
            exec.block_on(pipeline_task);
        });
}

#[bench]
fn shuffle_random(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    let exec = Box::pin(runtime);
    b.iter(|| {
            let pipeline = dummy_stream()
                .fork(THREADS, CHANNEL_BUFFER, &exec)
                .shuffle_unordered(|i|{i * 23}, THREADS, CHANNEL_BUFFER, &exec)
                .join_unordered(CHANNEL_BUFFER*THREADS, &exec);

            let pipeline_task = pipeline
            .for_each(|_item| async {
                ()
            });
            
            exec.block_on(pipeline_task);
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
    let runtime = Runtime::new().expect("can not start runtime");

    b.iter(|| {

        let task = future::lazy(|_| () );

        runtime.block_on(task);
    });
}

#[bench]
fn spawn_1(b: &mut Bencher) {

    b.iter(|| {
        let runtime = Runtime::new().expect("can not start runtime");
        let task = future::lazy(|_| () );
        runtime.block_on(task);
    });
}

#[bench]
fn spawn(b: &mut Bencher) {

    b.iter(|| {
        let runtime = Runtime::new().expect("can not start runtime");

        let mut tasks = Vec::with_capacity(BLOCK_COUNT);
        for _i in 0 .. BLOCK_COUNT {
            let task = future::lazy(|_| () );

            tasks.push(runtime.spawn(task));
        }

        runtime.block_on(future::join_all(tasks));
    });
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

#[bench]
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

#[bench]
fn latency_channel_buf10(_b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    for _i in 0 .. 5 {
        let (tx, rx) = channel::<Instant>(10);

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

#[bench]
fn latency_channel_buf100_latency(_b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    for _i in 0 .. 5 {
        let (tx, rx) = channel::<Instant>(100);

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

#[bench]
fn fold_merge(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    let exec = Box::pin(runtime);

    b.iter(|| {
        let pipeline_task = dummy_stream()
            .fork(THREADS, CHANNEL_BUFFER, &exec)
            .instrumented_fold(|| 0usize, |acc: usize, _val:Message | async move {
                    acc + 1usize
            }, "acc".to_owned())
            .merge(0, |acc, part_acc| async move {
                acc + part_acc
            }, &exec);
            
        let ops = exec.block_on(pipeline_task);
        assert_eq!(ops, BLOCK_COUNT);
    });

}

#[bench]
fn async_stream_latency(_b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    for _i in 0 .. 5 {
        let task = time_stream().fold((0u128, 0usize),
        |(sum, len), t| async move {
            let dt = t.elapsed().as_nanos();
            (sum + dt, len +1)
        })
        .map(|(sum, len)| {
            let avg_latency = sum as f64 / len as f64;
            eprintln!("~latency:{:0.1}ns", avg_latency)
        });

        runtime.block_on(task);
    }
}

#[bench]
fn async_read_codec(b: &mut Bencher) {
    use tokio_util::codec::{BytesCodec, FramedRead, /*FramedWrite*/};
    use tokio::fs::{File};
    let runtime = Runtime::new().expect("can not start runtime");

    b.iter(|| {
        let file_future = File::open("10Mdata");
        let file: tokio::fs::File = runtime
                .block_on(file_future)
                .expect("Can't open input file.");
        //    Box::pin(byte_stream)
        let input_stream = FramedRead::new(file , BytesCodec::new());
        let task = input_stream.for_each(|_b| async {()});
        runtime.block_on(task);
    });

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
