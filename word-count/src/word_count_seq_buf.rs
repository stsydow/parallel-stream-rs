//https://ptrace.fefe.de/wp/wpopt.rs

// gcc -o lines lines.c
// tar xzf llvm-8.0.0.src.tar.xz
// find llvm-8.0.0.src -type f | xargs cat | tr -sc 'a-zA-Z0-9_' '\n' | perl -ne 'print unless length($_) > 1000;' | ./lines > words.txt

use std::fs::File;
use std::io::{self, BufRead, BufReader, Stdin};
use std::iter::FromIterator;

use bytes::{Bytes, Buf};

use word_count::util::*;

#[inline(never)]
fn read_file<'a>(my_stdin: &'a Stdin) -> io::Result<Box<dyn BufRead + 'a>> {
    let conf = parse_args("word count simple");

    let io_box: Box<dyn BufRead> = match conf.input {
        Some(filename) => {
            let file = File::open(filename)?;
            Box::new(BufReader::new(file))
        }
        None => Box::new(my_stdin.lock()),
    };
    Ok(io_box)
}

#[inline(never)]
fn write_out(frequency: &Vec<(Bytes, u64)>) -> io::Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();

    // Push to stdout
    for (word_raw, count) in frequency {
        // Could also do println!("{} {}", word count) here,
        // but this is faster
        let word = utf8(&word_raw).expect("UTF8 encoding error");
        let out = &*format!("{} {}\n", word, count);
        io::copy(&mut out.as_bytes(), &mut stdout)?;
    }
    Ok(())
}

fn main() -> io::Result<()> {
    let my_stdin = io::stdin();
    let mut buffered = read_file(&my_stdin)?;

    let mut frequency = FreqTable::new();
    let mut remainder = Bytes::new();
    loop {
        let raw_buffer = buffered.fill_buf()?;
        let amount = raw_buffer.len() + remainder.remaining();
        if amount == 0 {
            break;
        }
        let buffer = remainder.chain(raw_buffer).copy_to_bytes(amount);
        let consumed = count_bytes(&mut frequency, &buffer);
        remainder = buffer.slice(consumed..);
        buffered.consume(amount);
    }

    // Sort by value
    let mut frequency = Vec::from_iter(frequency);
    frequency.sort_by(|&(_, a), &(_, b)| b.cmp(&a));

    write_out(&frequency)
}
