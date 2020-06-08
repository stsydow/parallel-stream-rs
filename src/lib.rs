#![feature(test)]
extern crate test;
//#![feature(impl_trait_in_bindings)]
//extern crate num_derive;

//pub mod error;

//pub mod logging;
//pub mod context;
//pub mod util;
//pub mod stream_group_by;
pub mod global_context;
mod stream_fork;
//mod stream_fork_chunked;
pub use crate::stream_fork::{ForkRR, Fork, fork_rr};
mod stream_join;
pub use crate::stream_join::{JoinTagged, Join};
pub mod stream_shuffle;
pub mod stream_shuffle_buffered;
//pub mod parallel_pipeline;
pub mod selective_context;
pub mod probe_stream;
mod log_histogram;
pub use crate::log_histogram::LogHistogram;
pub mod instrumented_map;
pub mod instrumented_fold;

mod stream_ext;
pub use crate::stream_ext::StreamExt;

use std::cmp::Ordering;
use std::time::{ Instant };
use tokio::prelude::*;
use tokio::sync::mpsc::{Receiver, channel};

#[cfg(test)]
mod tests;

pub struct Tag<I> {
    seq_nr: usize,
    data: I
}

pub fn tag<I>(seq_nr: usize, data:I) -> Tag<I>
{
    Tag{seq_nr, data}
}

impl<I> Tag<I> {
    pub fn untag(self) -> I {
        self.data
    }
}


impl<I> Ord for Tag<I> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.seq_nr.cmp(&other.seq_nr)
    }
}

impl<I> PartialOrd for Tag<I> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<I> PartialEq for Tag<I> {
    fn eq(&self, other: &Self) -> bool {
        self.seq_nr == other.seq_nr
    }
}

impl<I> Eq for Tag<I> {}

pub struct ParallelStream<S>
{
    streams: Vec<S>,
}

impl<S, I> ParallelStream<S>
where
    //I: Send + 'static,
    S: Stream<Item=Tag<I>>,// + Send + 'static,
{
    pub fn join(self) -> JoinTagged<S, I> {
        stream_join::join_tagged(self)
    }
}

impl<S, I> ParallelStream<S>
where
    I: Send + 'static,
    S: Stream<Item=I> + Send + 'static,
{

    pub fn decouple(self, buf_size: usize) ->  ParallelStream<Receiver<I>> {

        let mut streams = Vec::new();
        for input in self.streams {
            let (tx, rx) = channel::<I>(buf_size);
            streams.push(rx);

            let pipe_task = input
                .forward(tx.sink_map_err(|e| {
                    eprintln!("decouple in send error:{}", e);
                    panic!()
                }))
            .map(|(_in, _out)| ())
                .map_err(|_e| {
                    panic!()
                });

            tokio::spawn(pipe_task);
        }
        ParallelStream{ streams}
    }
}


impl<T: ?Sized, I> TimedStream<I> for T where T: Stream<Item=(Instant, I)> {}

pub trait TimedStream<I>: Stream<Item=(Instant, I)> {
    fn probe(self, name: String) -> probe_stream::Probe<Self>
        where Self: TimedStream<I>,
            Self: Sized
    {
        probe_stream::Probe::new(self, name)
    }

    fn map<U, F>(self, f: F) -> probe_stream::Map<Self, F>
        where F: FnMut(I) -> U,
              Self: Sized
    {
        probe_stream::Map::new(self, f)
    }

}


//TODO ????
/*pub trait ParallelStream
{
    type Stream;


}

>>> or as Struct?

pub struct ParallelStream<S,Sel>
{
    tagged: bool,
    partitioing:Option<Sel>,
    streams: Vec<S>,


}
*/

/* TODO
// see https://github.com/async-rs/parallel-stream/
  fork Stream -> ParallelStream

  pub trait ParallelStream {

        map()
        context()
        ?fold()

        join() -> Stream
        reorder()
  }


  pub trait TaggedStream {

  }
  fork(Stream) -> TaggedStream
  join!([TaggedStream]) -> Stream

 */

