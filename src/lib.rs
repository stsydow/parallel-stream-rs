#![feature(test)]
extern crate test;
//#![feature(impl_trait_in_bindings)]
//extern crate num_derive;

//pub mod error;

//pub mod logging;
//pub mod context;
//pub mod util;
//pub mod stream_group_by;
//mod global_context; //TODO
mod stream_fork;
pub use crate::stream_fork::{ForkRR, fork_rr, ForkSel, fork_sel};
mod stream_fork_chunked;
pub use crate::stream_fork_chunked::{ChunkedFork};
mod stream_join;
pub use crate::stream_join::{JoinTagged, Join};
//broken pub mod stream_shuffle;
//broken pub mod stream_shuffle_buffered;
//pub mod parallel_pipeline;
mod selective_context;
pub use crate::selective_context::{SelectiveContext, SelectiveContextBuffered};
mod log_histogram;
pub use crate::log_histogram::LogHistogram;
mod instrumented_map;
pub use crate::instrumented_map::{InstrumentedMap, InstrumentedMapChunked};
mod instrumented_fold;
pub use crate::instrumented_fold::InstrumentedFold;

mod probe_stream; //TODO Tag is defined twice
//pub use probe_stream::{Meter, Tag}
mod stream_ext;
pub use crate::stream_ext::{StreamExt, StreamChunkedExt};

mod parallel_stream;
pub use crate::parallel_stream::ParallelStream;

mod tagged_stream;
pub use crate::tagged_stream::{TaggedStream, Tag, tag};

//#[cfg(test)]
//mod tests;

use tokio::prelude::*;
use std::time::{ Instant };

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

