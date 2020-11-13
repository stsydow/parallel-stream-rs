use std::time::Instant;

use tokio::runtime::Runtime;
use futures::future::{FutureExt};
use futures::stream::{self, Stream, StreamExt};

use bencher::{benchmark_group, benchmark_main, Bencher, black_box};

use parallel_stream::StreamExt as MyStreamExt;
use parallel_stream::StreamChunkedExt;


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

fn iter_stream(b: &mut Bencher) {
    b.iter(|| {
        dummy_iter()
            .for_each(|item| {
                test_msg(item)
            });
    });
}

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

fn async_stream_smol(b: &mut Bencher) {
    b.iter(|| {
        let task = dummy_stream()
            .for_each(|item| async move {
                test_msg(item);
            });
        smol::block_on(task);
    });
}

fn async_stream_exec_fut(b: &mut Bencher) {
    use futures::executor::block_on;

    b.iter(|| {
        let task = dummy_stream()
            .for_each(|item| async move {
                test_msg(item);
            });
        block_on(task);
    });
}

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

fn selective_context_buf(b: &mut Bencher) {
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

benchmark_group!(async_fn, 
    sync_fn, iter_stream, 
    async_stream, async_stream_smol, async_stream_exec_fut,
    // async_stream_latency,
    async_stream_map10, async_chunk10, 
    selective_context_buf, selective_context
);

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

fn measure_time(b: &mut Bencher) {
    b.iter( || {
        let t = Instant::now();
        //let dt = t.elapsed()//.as_nanos();
        //dt
        t
    });
}

benchmark_group!(time, measure_time);

benchmark_main!(async_fn, time);
