use std::time::Instant;
//use bytes::Bytes;

use tokio::runtime::Runtime;
use futures::future::{self, FutureExt};
use futures::TryFutureExt;
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
const DEV_ZERO: &'static str = "/dev/zero"; 

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
        (0 .. BLOCK_COUNT).map(|i| new_message(i))
}

fn dummy_stream() -> impl Stream<Item=Message> {
    stream::iter(
        dummy_iter()
    )
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
fn async_chunk10(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
        let task = dummy_stream()
            .chunks(10)
            .for_each(|chunk| async {
                for item in chunk {
                    test_msg(item);
                }
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
fn selective_context(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
        let task = dummy_stream()
            .selective_context(
                |_| {0},
                |i| {*i},
                |cx, i| { *cx += i; i },
                "sel_test".to_owned()
                )
            .for_each(|item| async move {
                test_msg(item);
            });
        runtime.block_on(task);
    });
}

#[bench]
fn selective_context_buf(b: &mut Bencher) {
    use crate::stream_ext::StreamChunkedExt;
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
        let task = dummy_stream()
            .chunks(100)
            .selective_context_buffered(
                |_| {0},
                |i| {*i},
                |cx, i| { *cx += i; i },
                "sel_test".to_owned()
                )
            .for_each(|chunk| async move {
                for item in chunk {
                    test_msg(item);
                }
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

#[bench]
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

#[bench]
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

#[bench]
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

#[bench]
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

#[bench]
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

#[bench]
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

#[bench]
fn fork_join(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {

    let (fork, join) = {
        let mut senders = Vec::new();
        //let mut join = Join::new();
        let (out_tx, out_rx) = channel::<Message>(THREADS*CHANNEL_BUFFER);
        for _i in 0 .. THREADS {
            let (in_tx, in_rx) = channel::<Message>(CHANNEL_BUFFER);

            senders.push(in_tx);
            in_rx.forward_and_spawn(out_tx.clone(), &runtime);
        }

        let fork = fork_rr(senders);

        (fork, out_rx)
    };

    dummy_stream().forward_and_spawn(fork, &runtime);

    let recv_task = join
        .for_each(|_item| async {()});

        runtime.block_on(recv_task);
    });
}

#[bench]
fn fork_join_unorderd(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
            let pipeline = dummy_stream()
                .fork(THREADS, CHANNEL_BUFFER, &runtime)
                .join_unordered(CHANNEL_BUFFER*THREADS, &runtime);

            let pipeline_task = pipeline
            .for_each(|_item| async {()});
            
            runtime.block_on(pipeline_task);
    });
}

#[bench]
fn shuffle_1(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
            let pipeline = dummy_stream()
                .fork(THREADS, CHANNEL_BUFFER, &runtime)
                .shuffle_unordered(|i|{*i}, THREADS, CHANNEL_BUFFER, &runtime)
                .join_unordered(CHANNEL_BUFFER*THREADS, &runtime);

            let pipeline_task = pipeline
            .for_each(|_item| async {
                ()
            });
            
            runtime.block_on(pipeline_task);
        });
}

#[bench]
fn shuffle_unfair(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
            let pipeline = dummy_stream()
                .fork(THREADS, CHANNEL_BUFFER, &runtime)
                .shuffle_unordered(|_i|{0}, THREADS, CHANNEL_BUFFER, &runtime)
                .join_unordered(CHANNEL_BUFFER*THREADS, &runtime);

            let pipeline_task = pipeline
            .for_each(|_item| async {
                ()
            });
            
            runtime.block_on(pipeline_task);
        });
}

#[bench]
fn shuffle_nobuffer(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
            let pipeline = dummy_stream()
                .fork(THREADS, 1, &runtime)
                /*.map(|i| {
                    eprintln!("f{}", i);
                    i
                })*/
                .shuffle_unordered(|_i|{0}, THREADS, 1, &runtime)
                /*.map(|i| {
                    eprintln!("s{}", i);
                    i
                })*/
                .join_unordered(1, &runtime);

            let pipeline_task = pipeline
            .for_each(|_item| async {
                ()
            });
            
            runtime.block_on(pipeline_task);
        });
}

#[bench]
fn shuffle_random(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
            let pipeline = dummy_stream()
                .fork(THREADS, CHANNEL_BUFFER, &runtime)
                .shuffle_unordered(|i|{i * 23}, THREADS, CHANNEL_BUFFER, &runtime)
                .join_unordered(CHANNEL_BUFFER*THREADS, &runtime);

            let pipeline_task = pipeline
            .for_each(|_item| async {
                ()
            });
            
            runtime.block_on(pipeline_task);
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
            let mut runtime = DefaultExecutor::current();
            let input_stream = FramedRead::new(file, BytesCodec::new());
            let sub_table_streams: Receiver<Vec<(Bytes, u64)>> = input_stream
                .fork(THREADS, 2, &mut runtime)
                .instrumented_fold(|| FreqTable::new(), |mut frequency, text| {
                    let text = text.freeze();
                    count_bytes(&mut frequency, &text);

                    future::ok::<FreqTable, _>(frequency)
                }, "split_and_count".to_owned())
                .map_result(|frequency| Vec::from_iter(frequency))
                ... stream::iter_ok(frequency) ...
                //.map_err(|e| {panic!(); ()})
                .fork_sel_chunked( |(word, _count)| word.len() , THREADS, 2, &mut runtime )
                .instrumented_fold(|| FreqTable::new(), |mut frequency, chunk| {

                    for (word, count) in chunk {
                        *frequency.entry(word).or_insert(0) += count;
                    }

                    future::ok::<FreqTable, _>(frequency)
                }, "merge_table".to_owned()).map_result(|sub_table| {
                    Vec::from_iter(sub_table)
                } )
                //.merge( init, f, &mut runtime)
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
    let runtime = Runtime::new().expect("can not start runtime");
    
    b.iter(|| {
        let task = future::lazy(|_| () );
        runtime.block_on(task);
    });
}

#[bench]
fn spawn(b: &mut Bencher) {
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
fn latency_channel_buf_big_latency(_b: &mut Bencher) {
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

#[bench]
fn fold_merge(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");

    b.iter(|| {
        let pipeline_task = dummy_stream()
            .fork(THREADS, CHANNEL_BUFFER, &runtime)
            .instrumented_fold(|| 0usize, |acc: usize, _val:Message | async move {
                    acc + 1usize
            }, "acc".to_owned())
            .merge(0, |acc, part_acc| async move {
                acc + part_acc
            }, &runtime);
            
        let ops = runtime.block_on(pipeline_task);
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

const BUFFER_SIZE:usize = 4096; 
#[bench]
fn async_read_codec(b: &mut Bencher) {
    use tokio_util::codec::{BytesCodec, FramedRead, /*FramedWrite*/};
    use tokio::fs::File;
    let runtime = Runtime::new().expect("can not start runtime");

    b.iter(|| {
        let file_future = File::open("/dev/zero");
        let file: File = runtime
                .block_on(file_future)
                .expect("Can't open input file.");
        let input_stream = FramedRead::with_capacity(file , BytesCodec::new(), BUFFER_SIZE);
        let task = input_stream.take(BLOCK_COUNT).for_each(|_i| async {()});
        runtime.block_on(task);
    });

}

#[bench]
fn async_read_fs(b: &mut Bencher) {
    use async_fs::File;
    use futures::AsyncReadExt;
    let runtime = Runtime::new().expect("can not start runtime");

    b.iter(|| {
        let task = File::open("/dev/zero")
            .then(|r_file| async move {
                let mut file = r_file.expect("Can't open input file.");
                let mut buffer = [0u8; BUFFER_SIZE];

                for _i in 0 .. BLOCK_COUNT {
                    let count = file.read(&mut buffer).await
                        .expect("Can't read file.");
                    if count == 0 { break; }
                }
            });
       
        /*
        let input_stream = stream::unfold(file, |mut file| async move {
                let mut buffer = [0u8; BUFFER_SIZE];

                let count = file.read(&mut buffer[..]).await
                    .expect("Can't read file.");
                if count == 0 { 
                    return None;
                }
                assert_eq!(count, BUFFER_SIZE);
                let next_state = file;
                let yielded = buffer;
                Some( (yielded, next_state) )
        });
        let task = input_stream.take(BLOCK_COUNT).fold((), |(), ref _buffer | async {()});
        */
        runtime.block_on(task);
    });

}

#[bench]
fn async_read(b: &mut Bencher) {
    use tokio::fs::{File};
    use tokio::io::{AsyncReadExt};
    let runtime = Runtime::new().expect("can not start runtime");

    b.iter(|| {
        let task = || async { 
                let mut file = File::open(DEV_ZERO)
                    .await
                    .unwrap();
                let mut buffer = [0u8; BUFFER_SIZE];

                for _i in 0 .. BLOCK_COUNT {
                    let count = file.read(&mut buffer)
                        .await
                        .unwrap();
                    if count == 0 { break; }
                }
            };

       
        runtime.block_on(task());
    });
}

#[bench]
fn async_read_std_file(b: &mut Bencher) {
    use std::fs::File;
    use std::io::Read;
    let runtime = Runtime::new().expect("can not start runtime");
    
    b.iter(|| {
        let task = || async { 
                let mut file = 
                    tokio::task::block_in_place(||
                    Box::pin(File::open(DEV_ZERO).unwrap())
                    )
                ;
                

                for _i in 0 .. BLOCK_COUNT {
                    let mut buffer = [0u8; BUFFER_SIZE];
                    let mut file_ref = file.as_mut();

                    tokio::task::block_in_place(move || {
                        file_ref.read_exact(&mut buffer).unwrap();
                    });
                    //yield buffer;
                }

            };
       
        runtime.block_on(task());
    });
}

#[bench]
fn sync_read(b: &mut Bencher) {
    use std::fs::File;
    use std::io::Read;
    
    b.iter(|| {
        let mut file = File::open(DEV_ZERO).expect("Unable to open file");

        let mut buffer = [0u8; BUFFER_SIZE];

        for _i in 0 .. BLOCK_COUNT {
            file.read_exact(&mut buffer)
                .expect("err reading file");
        }
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
