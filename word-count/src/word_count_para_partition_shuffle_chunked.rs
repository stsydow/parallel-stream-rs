//https://ptrace.fefe.de/wp/wpopt.rs

// gcc -o lines lines.c
// tar xzf llvm-8.0.0.src.tar.xz
// find llvm-8.0.0.src -type f | xargs cat | tr -sc 'a-zA-Z0-9_' '\n' | perl -ne 'print unless length($_) > 1000;' | ./lines > words.txt
//#![feature(impl_trait_in_bindings)]

use std::fmt::Write;
use std::io;
use std::iter::FromIterator;

use bytes::{BufMut, Bytes, BytesMut};
use futures::future;
use futures::stream;
//use futures::Stream;
use tokio::codec::{BytesCodec, FramedRead, FramedWrite};
use tokio::prelude::*;
use tokio::runtime::Runtime;

use tokio::sync::mpsc::{channel, Receiver};

use std::cmp::max;
use parallel_stream::{fork_rr, StreamExt};
use word_count::stream_shuffle_buffered::ShuffleBuffered;
use tokio::sync::mpsc::error::SendError;

use std::time::Instant;
use word_count::util::*;

const BUFFER_SIZE: usize = 4;

//use futures::sync::mpsc::channel;

const CHUNKS_CAPACITY: usize = 256;

fn reduce_task<InItem, OutItem, S, FBuildPipeline, OutFuture, E>(
    src: Receiver<InItem>,
    sink: S,
    builder: FBuildPipeline,
) -> impl Future<Item = (), Error = ()>
where
    E: std::error::Error,
    S: Sink<SinkItem=OutItem, SinkError=SendError>,
    OutFuture: Future<Item = OutItem, Error = E>,
    FBuildPipeline: FnOnce(Receiver<InItem>) -> OutFuture,
{
    future::lazy(move || {
        let task = builder(src)
            .and_then(|result| {
                sink.sink_map_err(|e| panic!("send error: {:#?}", e))
                    .send(result)
            })
            .map(|_sink| ())
            .map_err(|e| panic!("pipe_err:{:#?}", e));
        task
    })
}

fn forward_task<InItem, OutItem, S, FBuildPipeline, OutStream, E>(
    src: Receiver<InItem>,
    sink: S,
    builder: FBuildPipeline,
) -> impl Future<Item = (), Error = ()>
    where
        E: std::error::Error,
        S: Sink<SinkItem=OutItem, SinkError=SendError>,
        OutStream: Stream<Item = OutItem, Error = E>,
        FBuildPipeline: FnOnce(Receiver<InItem>) -> OutStream,
{
    future::lazy(move || {
        let task = builder(src)
            .forward(sink.sink_map_err(|e| panic!("join send error: {}", e)))
            .map(|(_src, _sink)| ())
            .map_err(|e| panic!("pipe_err:{:?}", e));
        task
    })
}


//#[inline(never)]
fn count_fn(stream: Receiver<Bytes>) -> impl Future<Item=Vec<(Bytes, u64)>, Error = io::Error> {
    let item_stream = stream
        .instrumented_fold(FreqTable::new(), |mut frequency, text| {
            count_bytes(&mut frequency, &text);

            future::ok::<FreqTable, _>(frequency)
        }, "split_and_count".to_owned())
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("recv error: {:#?}", e)))
        .map(|frequency| Vec::from_iter(frequency) );
    item_stream
}

//#[inline(never)]
fn acc_fn(stream: Receiver<Vec<(Bytes, u64)>>) -> impl Future<Item=Vec<(Bytes, u64)>, Error = io::Error> {
    let part = stream
        .instrumented_fold(FreqTable::new(), |mut frequency, chunk| {

            for (word, count) in chunk {
                *frequency.entry(word).or_insert(0) += count;
            }

            future::ok::<FreqTable, _>(frequency)
        }, "merge_table".to_owned())
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("recv error: {:#?}", e)))
        .map(|sub_table| Vec::from_iter(sub_table) );
    part
}

fn main() -> io::Result<()> {
    let conf = parse_args("word count parallel buf");

    let pipe_threads = max(1, conf.threads);

    //let mut runtime = Runtime::new()?;
    use std::time::Duration;

    //use tokio_timer::clock::Clock;
    use tokio::runtime::Builder;
    let mut runtime = Builder::new()
        //.blocking_threads(pipe_threads/4+1)
        //.blocking_threads(2)
        //.clock(Clock::system())
        .core_threads(pipe_threads)
        .keep_alive(Some(Duration::from_secs(1)))
        //.name_prefix("my-custom-name-")
        //.stack_size(16 * 1024 * 1024)
        .build()?;

    let mut exec = runtime.executor();
    let (start_usr_time, start_sys_time) =  get_cputime_usecs();
    let start_time = Instant::now();

    let (input, output) = open_io_async(&conf);

    let input_stream = FramedRead::new(input, WholeWordsCodec::new());
    let output_stream = FramedWrite::new(output, BytesCodec::new());


    let join = {

        //let mut senders = Vec::new();
        //let mut join = Join::new(|(_word, count)| { *count});

        let mut sort_inputs = Vec::with_capacity(pipe_threads);
        let (out_tx, out_rx) = channel::<Vec<(Bytes, u64)>>(pipe_threads);

        for _i in 0..pipe_threads {
            let (tx, rx) = channel::<Vec<(Bytes, u64)>>(pipe_threads);
            sort_inputs.push(tx);
            let sort_task = reduce_task(rx, out_tx.clone(), acc_fn);
            runtime.spawn(sort_task);
        }

        let mut shuffle = ShuffleBuffered::new( |(word, _count)| word.len() , sort_inputs);

        let parallel_words = input_stream.fork(pipe_threads, BUFFER_SIZE, &mut exec);
        for s in Vec::from(parallel_words) {
            let sink = shuffle.create_input();
            let tables = s.instrumented_fold(FreqTable::new(), |mut frequency, text| {
                count_bytes(&mut frequency, &text);

                future::ok::<FreqTable, _>(frequency)
            }, "split_and_count".to_owned())
            .map(|frequency| Vec::from_iter(frequency) );

            let task = tables.and_then(|result| {
                sink.sink_map_err(|e| panic!("send error: {:#?}", e))
                    .send(result)
            })
            .map(|_sink| ())
            .map_err(|e| panic!("pipe_err:{:#?}", e));
            runtime.spawn(task);
        }
        /*
        for _i in 0..pipe_threads {
            let (in_tx, in_rx) = channel::<Bytes>(BUFFER_SIZE);

            senders.push(in_tx);
            let sort_tx = shuffle.create_input();
            let pipe = reduce_task(in_rx, sort_tx, count_fn);
            runtime.spawn(pipe);
        }

        let fork = fork_rr(senders);
        input_stream.forward_and_spawn(fork, &mut runtime.executor());
        */
        /*
        let parallel_words = input_stream.fork(pipe_threads, BUFFER_SIZE, &mut exec)
        .instrumented_fold(|| FreqTable::new(), |mut frequency, text| {
            count_bytes(&mut frequency, &text);

            future::ok::<FreqTable, _>(frequency)
        }, "split_and_count".to_owned())
        .map(|frequency| Vec::from_iter(frequency) );

        for s in Vec::from(parallel_words) {
            let sort_tx = shuffle.create_input();
            s.forward_and_spawn(sort_tx, &mut exec);
        }
        */

        out_rx
    };


    let task = join
        .fold(Vec::new(), |mut frequency, mut part| {
            frequency.append(&mut part);
            future::ok(frequency)
        })
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("recv error: {:#?}", e)))
        .map(|mut frequency| {
            let sort_time = Instant::now();
            frequency.sort_unstable_by(|(ref w_a, ref f_a), (ref w_b, ref f_b)| f_b.cmp(&f_a).then(w_b.cmp(&w_a)));
            eprintln!("sorttime:{:?}", sort_time.elapsed());
            stream::iter_ok(frequency).chunks(CHUNKS_CAPACITY)
        })
        .flatten_stream()
        .instrumented_map(
            |chunk: Vec<(Bytes, u64)>| {
            let mut buffer = BytesMut::with_capacity(CHUNKS_CAPACITY * 15);
            for (word_raw, count) in chunk {
                let word = utf8(&word_raw).expect("UTF8 encoding error");
                let max_len = word_raw.len() + 15;
                if buffer.remaining_mut() < max_len {
                    buffer.reserve(10 * max_len);
                }
                buffer
                    .write_fmt(format_args!("{} {}\n", word, count))
                    .expect("Formating error");
            }
            buffer.freeze()
        }, "format_chunk".to_owned())
        .forward(output_stream);

    let (_word_stream, _out_file) = runtime.block_on(task)?;

    let difference = start_time.elapsed();
    let (end_usr_time, end_sys_time) = get_cputime_usecs();
    let usr_time = (end_usr_time - start_usr_time) as f64 / 1000_000.0;
    let sys_time = (end_sys_time - start_sys_time) as f64 / 1000_000.0;
    eprintln!("walltime: {:?} (usr: {:.3}s sys: {:.3}s)",
        difference, usr_time, sys_time);
    runtime.shutdown_on_idle();

    Ok(())
}
