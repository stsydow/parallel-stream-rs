//https://ptrace.fefe.de/wp/wpopt.rs

// gcc -o lines lines.c
// tar xzf llvm-8.0.0.src.tar.xz
// find llvm-8.0.0.src -type f | xargs cat | tr -sc 'a-zA-Z0-9_' '\n' | perl -ne 'print unless length($_) > 1000;' | ./lines > words.txt

use std::collections::HashMap;
use std::io::Error as StdError;
use std::io::Result as StdResult;
use std::iter::FromIterator;

use bytes::Bytes;
use futures::future::FutureResult;
use tokio_util::codec::{BytesCodec, FramedRead, FramedWrite};
use tokio::prelude::*;
use tokio::runtime::Runtime;
use std::time::Instant;
use word_count::util::*;
use parallel_stream::{StreamExt};

fn main() -> StdResult<()> {
    let conf = parse_args("word count async");
    let mut runtime = Runtime::new()?;

    let (start_usr_time, start_sys_time) =  get_cputime_usecs();
    let start_time = Instant::now();

    let (input, output) = open_io_async(&conf);

    let input_stream = FramedRead::new(input, BytesCodec::new());
    let output_stream = FramedWrite::new(output, BytesCodec::new());

    let frequency: HashMap<Vec<u8>, u32> = HashMap::new();

    let dbg_future = input_stream
        .instrumented_fold(
            (frequency, Vec::<u8>::new()),
            |(mut frequency, mut remainder), buffer| {
                word_count_buf_indexed(&mut frequency, &mut remainder, &buffer);
                let result: FutureResult<_, StdError> = future::ok((frequency, remainder));
                result
            }
            ,"split_and_count".to_owned()
        )
        .map(|(mut frequency, remainder)| {
            if !remainder.is_empty() {
                *frequency.entry(remainder).or_insert(0) += 1;
            }
            frequency
        })
        .map(|frequency| {
            let mut frequency_vec = Vec::from_iter(frequency);
            frequency_vec.sort_unstable_by(|(ref w_a, ref f_a), (ref w_b, ref f_b)| f_b.cmp(&f_a).then(w_b.cmp(&w_a)));
            stream::iter_ok(frequency_vec)
        })
        .flatten_stream()
        .instrumented_map(
            |(word_raw, count):(Vec<u8>,_)| {
            let word = ::std::str::from_utf8(&word_raw).expect("UTF8 encoding error");
            Bytes::from(format!("{} {}\n", word, count))
            },
            "format".to_owned())
        .forward(output_stream);

    let (_, _output_stream) = runtime.block_on(dbg_future)?;

    let difference = start_time.elapsed();
    let (end_usr_time, end_sys_time) = get_cputime_usecs();
    let usr_time = (end_usr_time - start_usr_time) as f64 / 1000_000.0;
    let sys_time = (end_sys_time - start_sys_time) as f64 / 1000_000.0;
    eprintln!("walltime: {:?} (usr: {:.3}s sys: {:.3}s)",
        difference, usr_time, sys_time);
    runtime.shutdown_on_idle();

    Ok(())
}
