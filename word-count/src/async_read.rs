//https://ptrace.fefe.de/wp/wpopt.rs

// gcc -o lines lines.c
// tar xzf llvm-8.0.0.src.tar.xz
// find llvm-8.0.0.src -type f | xargs cat | tr -sc 'a-zA-Z0-9_' '\n' | perl -ne 'print unless length($_) > 1000;' | ./lines > words.txt

use std::io::Result as StdResult;

use tokio_util::codec::{BytesCodec, FramedRead, /*FramedWrite*/};
use word_count::util::*;
use std::time::Instant;
use futures::StreamExt;

const BUFFER_SIZE:usize = 16*4096; 
fn main() -> StdResult<()> {
    let conf = parse_args("word count async buf");
    let runtime = tokio::runtime::Runtime::new()?;
    //let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let (start_usr_time, start_sys_time) =  get_cputime_usecs();
    let start_time = Instant::now();

    let (input, _output) = open_io_async(&conf);

    let input_stream = FramedRead::with_capacity(input, BytesCodec::new(), BUFFER_SIZE);
    //let input_stream = FramedRead::new(input, WholeWordsCodec::new());
    /*
    let input_stream = FramedRead {
                    inner: framed_read2_with_buffer(
                    Fuse(input, BytesCodec::new()),
                    BytesMut::with_capacity(8129*8)
                    ),
                    };
    */
    let dbg_future = input_stream.for_each(|_b| async {()});
    let _r = runtime.block_on(dbg_future);

    /*
    let output_stream = FramedWrite::new(output, BytesCodec::new());

    let dbg_future = input_stream
        .map(|b| b.freeze() ).forward(output_stream);
    let (_, _output_stream) = runtime.block_on(dbg_future)?;
    */

    let difference = start_time.elapsed();
    let (end_usr_time, end_sys_time) = get_cputime_usecs();
    let usr_time = (end_usr_time - start_usr_time) as f64 / 1000_000.0;
    let sys_time = (end_sys_time - start_sys_time) as f64 / 1000_000.0;
    eprintln!("walltime: {:?} (usr: {:.3}s sys: {:.3}s)",
        difference, usr_time, sys_time);

    Ok(())
}
