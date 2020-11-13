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
use futures::Stream;
use tokio::codec::{BytesCodec, FramedRead, FramedWrite};
use tokio::prelude::*;
use tokio::runtime::Runtime;

use word_count::util::*;

use tokio::sync::mpsc::{channel, Receiver, Sender};

use std::cmp::max;
use parallel_stream::stream_fork::ForkRR;

use parallel_stream::probe_stream::*;
use parallel_stream::LogHistogram;
use std::time::Instant;

const BUFFER_SIZE: usize = 4;

//use futures::sync::mpsc::channel;

const CHUNKS_CAPACITY: usize = 256;

fn reduce_task<InItem, OutItem, FBuildPipeline, OutFuture, E>(
    src: Receiver<InItem>,
    sink: Sender<OutItem>,
    builder: FBuildPipeline,
) -> impl Future<Item = (), Error = ()>
where
    E: std::error::Error,
    OutFuture: Future<Item = OutItem, Error = E>,
    FBuildPipeline: FnOnce(Receiver<InItem>) -> OutFuture,
{
    future::lazy(move || {
        let task = builder(src)
            .and_then(|result| {
                sink.sink_map_err(|e| panic!("join send error: {}", e))
                    .send(result)
            })
            .map(|_sink| ())
            .map_err(|e| panic!("pipe_err:{:?}", e));
        task
    })
}

#[inline(never)]
fn task_fn(stream: Receiver<Vec<Bytes>>) -> impl Future<Item = FreqTable, Error = io::Error> {
    let in_stream = stream.map_err(|e| io::Error::new(io::ErrorKind::Other, format!("recv error: {:#?}", e)));
    let table_future = Tag::new(in_stream)
        .fold((LogHistogram::new(), FreqTable::new()),
            |(mut hist, mut frequency), (tag_time, chunk)| {
            for word in chunk {
                if !word.is_empty() {
                    *frequency.entry(word).or_insert(0) += 1;
                }
            }
            hist.sample_now(&tag_time);
            future::ok::<(LogHistogram, FreqTable), io::Error>((hist, frequency))
        }).map(|(hist, frequency)| {
            hist.print_stats("count");
            frequency
        });
    table_future
}

fn main() -> io::Result<()> {
    let conf = parse_args("word count parallel chunked");
    let mut runtime = Runtime::new()?;

    let (input, output) = open_io_async(&conf);

    let input_stream = FramedRead::new(input, WordVecCodec::new());
    let output_stream = FramedWrite::new(output, BytesCodec::new());

    let (fork, join) = {
        let mut senders = Vec::new();
        //let mut join = Join::new(|(_word, count)| { *count});

        //let pipe_theards = max(1, conf.threads - 1); // discount I/O Thread
        let pipe_theards = max(1, conf.threads);
        let (out_tx, out_rx) = channel::<FreqTable>(pipe_theards);
        for _i in 0..pipe_theards {
            let (in_tx, in_rx) = channel::<Vec<Bytes>>(BUFFER_SIZE);

            senders.push(in_tx);
            let pipe = reduce_task(in_rx, out_tx.clone(), task_fn);
            runtime.spawn(pipe);
            //join.add(out_rx);
        }

        let fork = ForkRR::new(senders);

        (fork, out_rx)
    };

    let file_reader = input_stream
        .forward(fork.sink_map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("fork send error: {}", e))
        }))
        .map(|(_in, _out)| ())
        .map_err(|e| {
            eprintln!("error: {}", e);
            panic!()
        });
    runtime.spawn(file_reader);

    let sub_table_stream /*: impl Stream<Item=HashMap<Vec<u8>, u64>, Error=io::Error> + Send*/ = join
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("recv error: {:#?}", e)));

    let table_future = Tag::new(sub_table_stream)
        .fold((LogHistogram::new(), FreqTable::new()), |(mut hist, mut frequency), (tag_time, mut sub_table)| {
            for (word, count) in sub_table.drain() {
                *frequency.entry(word).or_insert(0) += count;
            }
            hist.sample_now(&tag_time);
            future::ok::<(LogHistogram, FreqTable), io::Error>((hist, frequency))
        })
        .map(|(hist, frequency)| {
            hist.print_stats("merge");
            frequency
        });

    let sort_stream = table_future.map(|mut frequency| {
            let start_time = Instant::now();
            let mut frequency_vec = Vec::from_iter(frequency.drain());
            frequency_vec.sort_by(|&(_, a), &(_, b)| b.cmp(&a));
            let difference = start_time.elapsed();
            println!("[sort] {:?}", difference);
            stream::iter_ok(frequency_vec).chunks(CHUNKS_CAPACITY) // <- TODO performance?
        });
    let file_writer = sort_stream
        .flatten_stream()
        .map(|chunk| {
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
            //TODO FIXME dropping output for profiling
            buffer.truncate(0);
            buffer.freeze()
        })
        .forward(output_stream);

    let (_word_stream, _out_file) = runtime.block_on(file_writer)?;

    runtime.shutdown_on_idle();

    Ok(())
}
