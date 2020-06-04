use argparse::{ArgumentParser, Print, Store, StoreOption};
use std::collections::HashMap;
use std::{io, str, usize};

use tokio::fs::{File, OpenOptions};
use tokio::io::{stdin, stdout};
use tokio::prelude::*;
use tokio::runtime::Runtime;

use bytes::{Bytes, BytesMut};
use tokio::codec::Decoder;

pub struct RawWordCodec {}

impl RawWordCodec {
    pub fn new() -> Self {
        RawWordCodec {}
    }
}

impl Decoder for RawWordCodec {
    type Item = BytesMut;
    // TODO: in the next breaking change, this should be changed to a custom
    // error type that indicates the "max length exceeded" condition better.
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<BytesMut>, io::Error> {
        if buf.len() >= 1_000_000 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("max word length exceeded {:#?}B", buf.len()),
            ));
        }

        let word_offset = buf //[self.last_cursor..] // TODO use last cursor
            .iter()
            .position(|b| b.is_ascii_whitespace());

        let some_word = if let Some(offset) = word_offset {
            // Found a line!
            let whitespace_index = offset;
            let mut word = buf.split_to(whitespace_index + 1);

            /*
            let word = &word[..word.len() - 1];
            let word = utf8(word)?;

            Some(word.to_string())
            */
            let _space = word.split_off(word.len() - 1);
            Some(word)
        } else {
            None
        };

        return Ok(some_word);
    }

    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<BytesMut>, io::Error> {
        Ok(match self.decode(buf)? {
            Some(frame) => Some(frame),
            None => {
                // No terminating newline - return remaining data, if any
                if buf.is_empty() {
                    None
                } else {
                    let word = buf.take();
                    Some(word)
                }
            }
        })
    }
}

pub struct WordVecCodec {}

impl WordVecCodec {
    pub fn new() -> Self {
        WordVecCodec {}
    }
}

const APPROX_WORD_LEN: usize = 8;

impl Decoder for WordVecCodec {
    type Item = Vec<Bytes>;
    // TODO: in the next breaking change, this should be changed to a custom
    // error type that indicates the "max length exceeded" condition better.
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Vec<Bytes>>, io::Error> {
        if buf.len() >= 1_000_000 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("max word length exceeded {:#?}B", buf.len()),
            ));
        }

        let last_space = buf //[self.last_cursor..] // TODO use last cursor
            .iter()
            .rposition(|b| b.is_ascii_whitespace());

        let some_words = if let Some(last_space_idx) = last_space {
            let mut complete_words = buf.split_to(last_space_idx + 1).freeze();
            let mut words = Vec::with_capacity(complete_words.len() / APPROX_WORD_LEN);
            loop {
                let mut white_space_idx = complete_words.len();
                for i in 0..complete_words.len() {
                    if complete_words[i].is_ascii_whitespace() {
                        white_space_idx = i;
                        break;
                    }
                }

                if white_space_idx < complete_words.len() {
                    let mut word = complete_words.split_to(white_space_idx + 1);
                    let _space = word.split_off(word.len() - 1);
                    words.push(word);
                } else {
                    break;
                }
            }

            Some(words)
        } else {
            None
        };

        return Ok(some_words);
    }

    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<Vec<Bytes>>, io::Error> {
        Ok(match self.decode(buf)? {
            Some(frame) => Some(frame),
            None => {
                // No terminating newline - return remaining data, if any
                if buf.is_empty() {
                    None
                } else {
                    let mut word = Vec::with_capacity(1);
                    word.push(buf.take().freeze());
                    Some(word)
                }
            }
        })
    }
}

pub struct WholeWordsCodec {}

impl WholeWordsCodec {
    pub fn new() -> Self {
        WholeWordsCodec {}
    }
}

impl Decoder for WholeWordsCodec {
    type Item = Bytes;
    // TODO: in the next breaking change, this should be changed to a custom
    // error type that indicates the "max length exceeded" condition better.
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Bytes>, io::Error> {
        if buf.len() >= 1_000_000 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("max word length exceeded {:#?}B", buf.len()),
            ));
        }

        let last_space = buf //[self.last_cursor..] // TODO use last cursor
            .iter()
            .rposition(|b| b.is_ascii_whitespace());

        let some_words = if let Some(last_space_idx) = last_space {
            let complete_words = buf.split_to(last_space_idx + 1);
            Some(complete_words.freeze())
        } else {
            None
        };

        return Ok(some_words);
    }

    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<Bytes>, io::Error> {
        Ok(match self.decode(buf)? {
            Some(frame) => Some(frame),
            None => {
                // No terminating newline - return remaining data, if any
                if buf.is_empty() {
                    None
                } else {
                    let word = buf.take().freeze();
                    Some(word)
                }
            }
        })
    }
}

use libc::{c_long, rusage, suseconds_t, timeval, time_t, getrusage, RUSAGE_SELF};

pub fn get_cputime_usecs() -> (u64, u64) {
    let mut usage = rusage {
        ru_utime: timeval{ tv_sec: 0 as time_t, tv_usec: 0 as suseconds_t, },
        ru_stime: timeval{ tv_sec: 0 as time_t, tv_usec: 0 as suseconds_t, },
        ru_maxrss: 0 as c_long,
        ru_ixrss: 0 as c_long,
        ru_idrss: 0 as c_long,
        ru_isrss: 0 as c_long,
        ru_minflt: 0 as c_long,
        ru_majflt: 0 as c_long,
        ru_nswap: 0 as c_long,
        ru_inblock: 0 as c_long,
        ru_oublock: 0 as c_long,
        ru_msgsnd: 0 as c_long,
        ru_msgrcv: 0 as c_long,
        ru_nsignals: 0 as c_long,
        ru_nvcsw: 0 as c_long,
        ru_nivcsw: 0 as c_long,
    };

    unsafe { getrusage(RUSAGE_SELF, (&mut usage) as *mut rusage); }

    let u_secs = usage.ru_utime.tv_sec as u64;
    let u_usecs = usage.ru_utime.tv_usec as u64;
    let s_secs = usage.ru_stime.tv_sec as u64;
    let s_usecs = usage.ru_stime.tv_usec as u64;

    let u_time = (u_secs * 1_000_000) + u_usecs;
    let s_time = (s_secs * 1_000_000) + s_usecs;

    (u_time, s_time)
}

pub struct Config {
    pub output: Option<String>,
    pub input: Option<String>,
    pub threads: usize,
}

pub fn parse_args(description: &str) -> Config {
    let mut conf: Config = Config {
        output: None,
        input: None,
        threads: 1,
    };

    {
        // this block limits scope of borrows by ap.refer() method
        let mut ap = ArgumentParser::new();

        ap.set_description(description);
        ap.add_option(
            &["-V", "--version"],
            Print(env!("CARGO_PKG_VERSION").to_string()),
            "Show version",
        );

        ap.refer(&mut conf.input)
            .add_argument("input", StoreOption, "input file - default: stdin");

        ap.refer(&mut conf.output).add_argument(
            "output",
            StoreOption,
            "output file - default: stdout",
        );

        ap.refer(&mut conf.threads).add_option(
            &["-t", "--threads"],
            Store,
            "thread count - default: 1",
        );

        ap.parse_args_or_exit();
    }

    return conf;
}

#[inline(never)]
pub fn open_io_async(conf: &Config) -> (Box<dyn AsyncRead + Send>, Box<dyn AsyncWrite + Send>) {
    let mut runtime = Runtime::new().expect("can't start async runtime");
    let input: Box<dyn AsyncRead + Send> = match &conf.input {
        None => Box::new(stdin()),
        Some(filename) => {
            let file_future = File::open(filename.clone());
            let byte_stream = runtime
                .block_on(file_future)
                .expect("Can't open input file.");
            Box::new(byte_stream)
        }
    };

    let output: Box<dyn AsyncWrite + Send> = match &conf.output {
        None => Box::new(stdout()),
        Some(filename) => {
            let file_future = OpenOptions::new()
                .write(true)
                .create(true)
                .open(filename.clone());
            let byte_stream = runtime
                .block_on(file_future)
                .expect("Can't open output file.");
            Box::new(byte_stream)
        }
    };

    (input, output)
}

pub type FreqTable = HashMap<Bytes, u64>;

#[inline(never)]
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

pub fn utf8(buf: &[u8]) -> Result<&str, io::Error> {
    str::from_utf8(buf)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Unable to decode input as UTF8"))
}

pub fn count_word<T>(frequency: &mut HashMap<Vec<T>, u32>, word: &[T])
where
    T: std::hash::Hash + std::cmp::Eq + std::clone::Clone,
{
    if word.is_empty() {
        return;
    }

    *frequency.entry(word.to_vec()).or_insert(0) += 1;
    /*
    let value = frequency.get_mut(word);
    if let Some(count) = value {
        *count += 1;
    } else {
        frequency.insert(Vec::from(word), 1);
    }
    */
}

pub fn word_count_buf_split(
    frequency: &mut HashMap<Vec<u8>, u32>,
    remainder: &mut Vec<u8>,
    buffer: &[u8],
) {
    let mut chunks: std::slice::Split<_, _> = buffer.split(|c| c.is_ascii_whitespace());
    let first_tail = chunks.next().unwrap();
    remainder.extend_from_slice(first_tail);
    let first_word = remainder.clone();
    let mut last_word = first_word.as_slice();
    for word in chunks {
        count_word(frequency, last_word);
        last_word = word;
    }

    remainder.clear();
    remainder.extend_from_slice(last_word);
}

pub fn word_count_buf_by_foot(
    frequency: &mut HashMap<Vec<u8>, u32>,
    remainder: &mut Vec<u8>,
    buffer: &[u8],
) {
    let current_word = remainder;
    current_word.reserve(20);

    for c in buffer {
        if c.is_ascii_whitespace() {
            count_word(frequency, current_word.as_ref());
            current_word.clear();
        } else {
            current_word.push(*c);
        }
    }
}

pub fn word_count_buf_indexed(
    frequency: &mut HashMap<Vec<u8>, u32>,
    remainder: &mut Vec<u8>,
    buffer: &[u8],
) {
    let mut i_start: usize = 0;

    for i in 0..buffer.len() {
        if buffer[i].is_ascii_whitespace() {
            remainder.extend_from_slice(&buffer[i_start..i]);
            count_word(frequency, remainder.as_ref());
            i_start = i + 1;
            remainder.clear();
            break;
        }
    }

    for i in i_start..buffer.len() {
        if buffer[i].is_ascii_whitespace() {
            count_word(frequency, &buffer[i_start..i]);
            i_start = i + 1;
        }
    }
    remainder.extend_from_slice(&buffer[i_start..]);
}
