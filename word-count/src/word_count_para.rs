//https://ptrace.fefe.de/wp/wpopt.rs

// gcc -o lines lines.c
// tar xzf llvm-8.0.0.src.tar.xz
// find llvm-8.0.0.src -type f | xargs cat | tr -sc 'a-zA-Z0-9_' '\n' | perl -ne 'print unless length($_) > 1000;' | ./lines > words.txt

use std::collections::HashMap;
use std::io;
use std::iter::FromIterator;

use bytes::{Bytes, BytesMut};
use futures::future::FutureResult;
use tokio_util::codec::{BytesCodec, FramedRead, FramedWrite};
use tokio::fs::{File, OpenOptions};
use tokio::io::{stdin, stdout};
use tokio::prelude::*;
use tokio::runtime::Runtime;
use word_count::util::*;

fn main() -> io::Result<()> {
    let conf = parse_args("word count parallel");
    let mut runtime = Runtime::new()?;

    let input: Box<dyn AsyncRead + Send> = match conf.input {
        None => Box::new(stdin()),
        Some(filename) => {
            let file_future = File::open(filename);
            let byte_stream = runtime
                .block_on(file_future)
                .expect("Can't open input file.");
            Box::new(byte_stream)
        }
    };

    let output: Box<dyn AsyncWrite + Send> = match conf.output {
        None => Box::new(stdout()),
        Some(filename) => {
            let file_future = OpenOptions::new().write(true).create(true).open(filename);
            let byte_stream = runtime
                .block_on(file_future)
                .expect("Can't open output file.");
            Box::new(byte_stream)
        }
    };

    let input_stream = FramedRead::new(input, BytesCodec::new());
    let output_stream = FramedWrite::new(output, BytesCodec::new());

    use tokio::sync::mpsc::channel;
    //use futures::sync::mpsc::channel;
    let (in_tx, in_rx) = channel::<BytesMut>(32);

    let file_reader =
        input_stream
            .forward(in_tx.sink_map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("send error: {}", e))
            }))
            .map(|(_in, _out)| ())
            .map_err(|e| {
                eprintln!("error: {}", e);
                panic!()
            });
    runtime.spawn(file_reader);

    let frequency: HashMap<Vec<u8>, u32> = HashMap::new();

    let (out_tx, out_rx) = channel::<(Vec<u8>, u32)>(1024);

    let processor = in_rx
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("recv error: {:#?}", e)))
        .fold(
            (frequency, Vec::<u8>::new()),
            |(mut frequency, mut remainder), buffer| {
                word_count_buf_indexed(&mut frequency, &mut remainder, &buffer);

                let result: FutureResult<_, io::Error> = future::ok((frequency, remainder));
                result
            },
        )
        .map(|(mut frequency, remainder)| {
            if !remainder.is_empty() {
                *frequency.entry(remainder).or_insert(0) += 1;
            }
            frequency
        })
        .map(|frequency| {
            let mut frequency_vec = Vec::from_iter(frequency);
            frequency_vec.sort_by(|&(_, a), &(_, b)| b.cmp(&a));
            stream::iter_ok(frequency_vec)
        })
        .and_then(|word_freq_stream| {
            word_freq_stream
                .forward(out_tx.sink_map_err(|e| {
                    io::Error::new(io::ErrorKind::Other, format!("send error: {}", e))
                }))
                .map(|(_word_stream, _writer)| ())
        })
        .map_err(|e| {
            eprintln!("error: {}", e);
            panic!()
        });

    runtime.spawn(processor);

    let file_writer = out_rx
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("recv error: {:#?}", e)))
        .map(|(word_raw, count)| {
            let word = ::std::str::from_utf8(&word_raw).expect("UTF8 encoding error");
            Bytes::from(format!("{} {}\n", word, count))
        })
        .forward(output_stream)
        .map(|(_word_stream, _out_file)| ());

    runtime.block_on(file_writer)?;

    runtime.shutdown_on_idle();

    Ok(())
}
