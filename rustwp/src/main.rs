extern crate crossbeam;
extern crate memmap;
extern crate time;

use memmap::MmapOptions;

use std::cmp;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{self};
use std::iter::FromIterator;

type CounterType = u32;

fn tokenize_part(buffer: &[u8]) -> HashMap<&[u8], CounterType> {
    let mut frequency : HashMap<&[u8], CounterType> = HashMap::new();
    let mut i = 0;
    let to = buffer.len();
    while i < to {
        while i < to && is_whitespace(buffer[i]) {
            i += 1;
        }
        let mut j = i;
        while j < to && !is_whitespace(buffer[j]) {
            j += 1;
        }
        if i == to || j == to {
            break;
        }
        let key = &buffer[i..j];
        *frequency.entry(key).or_insert(0) += 1;
        i = j;
    }
    return frequency;
}

fn is_whitespace(c: u8) -> bool {
    c == 0x20 || c == 0x0c || c == 0x0a || c == 0x0d || c == 0x09
}

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    let mut num_threads : usize = 4;
    if args.len() == 3 {
        num_threads = args[1].parse().expect("parsing error: <thread count> must be a positive number");
    } else {
        println!("usage: rustwp <thread count> <infile>")
    }

    let file = File::open(&args[2]).unwrap(); // Don't know how to mmap stdin
    let buffer = unsafe { MmapOptions::new().map(&file)? };

    let length = buffer.len();
    let part_size = length / num_threads;

    let mut from: usize = 0;
    let frequencies = crossbeam::scope(|scope| {
        let mut spawned_threads = vec![];
        for _ in 0..num_threads {
            let mut to = from + part_size;
            while to < length && !is_whitespace(buffer[to]) {
                to += 1;
            }
            let capped_to = cmp::min(to, length);
            let slice = &buffer[from..capped_to];
            spawned_threads.push(scope.spawn(move |_| tokenize_part(&slice)));
            from = to;
        }
        let mut merged_frequencies : HashMap<&[u8], CounterType> = HashMap::new();
        let part_frequencies = spawned_threads.into_iter().map(|thread| thread.join().unwrap());
        for part_frequency in part_frequencies {
            for (key, value) in part_frequency {
                *merged_frequencies.entry(key).or_insert(0) += value;
            }
        }
        merged_frequencies
    }).unwrap();

    // Sort by value
    let mut frequencies = Vec::from_iter(frequencies);
    frequencies.sort_by(|&(_, a), &(_, b)| b.cmp(&a));

    let stdout = io::stdout();
    let mut stdout = stdout.lock();

    // Push to stdout
    for (word, count) in frequencies {
        let out = &*format!("{} {}\n", std::str::from_utf8(word).unwrap(), count);
        io::copy(&mut out.as_bytes(), &mut stdout)?;
    }

    Ok(())
}
