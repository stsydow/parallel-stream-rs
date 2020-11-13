//https://ptrace.fefe.de/wp/wpopt.rs

// gcc -o lines lines.c
// tar xzf llvm-8.0.0.src.tar.xz
// find llvm-8.0.0.src -type f | xargs cat | tr -sc 'a-zA-Z0-9_' '\n' | perl -ne 'print unless length($_) > 1000;' | ./lines > words.txt

use std::fmt::Write;
use std::io::Result as StdResult;
use std::iter::FromIterator;

use bytes::{BufMut, Bytes, BytesMut};
use tokio_util::codec::{BytesCodec, FramedRead, FramedWrite};
use word_count::util::*;
use std::time::Instant;
use futures::StreamExt;
use futures::FutureExt;
use parallel_stream::StreamExt as MyStreamExt;

const CHUNKS_CAPACITY: usize = 1024;
const BUFFER_SIZE:usize = 16*4096; 

fn main() -> StdResult<()> {
    let conf = parse_args("word count async buf");
    let runtime = tokio::runtime::Runtime::new()?;
    //let runtime = tokio::runtime::Builder::new_current_thread().build()?;

    let (start_usr_time, start_sys_time) =  get_cputime_usecs();
    let start_time = Instant::now();

    let (input, output) = open_io_async(&conf);

    let input_stream = FramedRead::with_capacity(input, WholeWordsCodec::new(), BUFFER_SIZE);
    let output_stream = FramedWrite::new(output, BytesCodec::new());

    let freq_stream = input_stream
        .instrumented_fold(FreqTable::new(), |mut frequency, text| async move {
            count_bytes(&mut frequency, &text.expect("error reading input stream."));

            frequency
        },"split_and_count".to_owned())
        .map(|frequency| {
            let sort_time = Instant::now();
            let mut frequency_vec = Vec::from_iter(frequency);
            frequency_vec.sort_unstable_by(|(ref w_a, ref f_a), (ref w_b, ref f_b)| f_b.cmp(&f_a).then(w_b.cmp(&w_a)));
            eprintln!("sorttime:{:?}", sort_time.elapsed());
            futures::stream::iter(frequency_vec).chunks(CHUNKS_CAPACITY)
        })
        .flatten_stream()
        .instrumented_map(|chunk: Vec<(Bytes, u64)>| {
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
        }, "format_chunk".to_owned());
    let task = MyStreamExt::forward(freq_stream, output_stream);

    runtime.block_on(task)?;
    let difference = start_time.elapsed();
    let (end_usr_time, end_sys_time) = get_cputime_usecs();
    let usr_time = (end_usr_time - start_usr_time) as f64 / 1000_000.0;
    let sys_time = (end_sys_time - start_sys_time) as f64 / 1000_000.0;
    eprintln!("walltime: {:?} (usr: {:.3}s sys: {:.3}s)",
        difference, usr_time, sys_time);

    Ok(())
}
