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
pub use crate::stream_fork::{ForkRR, Fork, fork_rr};
pub mod stream_join;
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

use std::time::{ Instant };
use tokio::prelude::*;

#[cfg(test)]
mod tests;

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

