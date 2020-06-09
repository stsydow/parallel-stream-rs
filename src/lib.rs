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
pub use crate::stream_fork::{ForkRR, ForkSel};
mod stream_join;
pub use crate::stream_join::{JoinTagged, Join};
//broken pub mod stream_shuffle;
//broken pub mod stream_shuffle_buffered;
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
use std::hash::Hash;
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

    /*
    fn selective_context_buffered<Event, R, Key, Ctx, CtxInit, FSel, FWork>(
        self,
        ctx_builder: CtxInit,
        selector: FSel,
        work: FWork,
        name: String
    ) -> selective_context::SelectiveContextBuffered<Key, Ctx, Self, CtxInit, FSel, FWork>
    where
        //Ctx:Context<Event=Event, Result=R>,
        Key: Ord + Hash,
        CtxInit: Fn(&Key) -> Ctx,
        FSel: Fn(&Event) -> Key,
        FWork: Fn(&mut Ctx, &Event) -> R,
        Self: Sized + Stream<Item=Tag<Vec<Event>>>,
    {
        selective_context::selective_context_buffered(self, ctx_builder, selector, work, name)
    }
    */

    pub fn shuffle<FSel>(self, selector: FSel, degree: usize) ->
        ParallelStream<impl Stream<Item=Tag<I>>>
    where
        I:Send,
        S: Send + 'static,
        FSel: Fn(&I) -> usize + Copy + Send + 'static,
    {
        let input_degree = self.streams.len();
        let mut joins_streams = Vec::with_capacity(degree);
        for _i in 0 .. degree {
            joins_streams.push( Vec::with_capacity(input_degree))
        }

        for input in self.streams {
            let splits = input.fork_sel( move |t| (selector)(&t.data), degree);


            let mut i = 0;
            for s in splits.streams {
                joins_streams[i].push(s);
                i += 1;
            }
        }

        let mut joins = Vec::with_capacity(degree);
        for streams in joins_streams {
            joins.push(ParallelStream{streams}.join())
        }

        ParallelStream{ streams:joins }
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

impl<S: Stream> ParallelStream<S> //TODO this is untagged -- we need a tagged version
{
    fn instrumented_map_chunked<I, U, F>(self, f: F, name:String)
        -> ParallelStream<instrumented_map::InstrumentedMapChunked<S, F>>
        where S:Stream<Item=Vec<I>>,
              F: FnMut(I) -> U + Copy,
    {
        let mut streams = Vec::new();
        for input in self.streams {
            let map = input.instrumented_map_chunked(f, name.clone());
            streams.push(map);
        }
        ParallelStream{ streams }
    }

    fn instrumented_map<I, U, F>(self, f: F, name:String)
        -> ParallelStream<instrumented_map::InstrumentedMap<S, F>>
        where S:Stream<Item=I>,
              F: FnMut(I) -> U + Copy,
    {
        let mut streams = Vec::new();
        for input in self.streams {
            let map = input.instrumented_map(f, name.clone());
            streams.push(map);
        }
        ParallelStream{ streams }
    }

    // TODO only useful after shuffle
    fn selective_context_buffered<Event, R, Key, Ctx, CtxInit, FSel, FWork>(
        self,
        ctx_builder: CtxInit,
        selector: FSel,
        work: FWork,
        name: String
    ) -> ParallelStream<selective_context::SelectiveContextBuffered<Key, Ctx, S, CtxInit, FSel, FWork>>
    where
        //Ctx:Context<Event=Event, Result=R>,
        Key: Ord + Hash,
        CtxInit: Fn(&Key) -> Ctx + Copy,
        FSel: Fn(&Event) -> Key + Copy,
        FWork: Fn(&mut Ctx, &Event) -> R + Copy,
        S: Sized + Stream<Item=Vec<Event>>,
    {
        let mut streams = Vec::new();
        for input in self.streams {
            let map = input.selective_context_buffered(ctx_builder, selector, work, name.clone());
            //instrumented_map(f, name.clone());
            streams.push(map);
        }
        ParallelStream{ streams }
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

