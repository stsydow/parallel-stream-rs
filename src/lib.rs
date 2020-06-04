//#![feature(impl_trait_in_bindings)]
//extern crate num_derive;

//pub mod error;

pub mod logging;
//pub mod context;
pub mod util;
//pub mod stream_group_by;
pub mod global_context;
pub mod stream_fork;
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

use std::hash::Hash;
use std::time::{ Instant };
use tokio::prelude::*;

impl<T: ?Sized> StreamExt for T where T: Stream {}

pub trait StreamExt: Stream {
    // Instrumentation
    fn instrumented_map<U, F>(self, f: F, name: String) -> instrumented_map::InstrumentedMap<Self, F>
        where F: FnMut(Self::Item) -> U,
              Self: Sized
    {
        instrumented_map::new(self, f, name)
    }

    fn instrumented_map_chunked<I, U, F>(self, f: F, name: String) -> instrumented_map::InstrumentedMapChunked<Self, F>
        where Self:Stream<Item=Vec<I>>,
              F: FnMut(I) -> U,
              Self: Sized
    {
        instrumented_map::new_chunked(self, f, name)
    }

    fn instrumented_fold<Fut, T, F>(self, init:T, f:F, name: String) -> instrumented_fold::InstrumentedFold<Self, F, Fut, T>
        where F: FnMut(T, Self::Item) -> Fut,
              Fut: IntoFuture<Item = T>,
              Self::Error: From<Fut::Error>,
              Self: Sized,

    {
        instrumented_fold::new(self, f, init, name)
    }


    //TODO instrument ; add name
    fn selective_context<R, Key, Ctx, CtxInit, FSel, FWork>(
        self,
        ctx_builder: CtxInit,
        selector: FSel,
        work: FWork,
        name: String
    ) -> selective_context::SelectiveContext<Key, Ctx, Self, CtxInit, FSel, FWork>
    where
        //Ctx:Context<Event=Event, Result=R>,
        Key: Ord + Hash,
        CtxInit: Fn(&Key) -> Ctx,
        FSel: Fn(&Self::Item) -> Key,
        FWork: Fn(&mut Ctx, &Self::Item) -> R,
        Self: Sized,
    {
        selective_context::selective_context(self, ctx_builder, selector, work, name)
    }

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
        Self: Sized + Stream<Item=Vec<Event>>,
    {
        selective_context::selective_context_buffered(self, ctx_builder, selector, work, name)
    }

    fn fork(self, degree: usize) -> Vec<tokio::sync::mpsc::Receiver<Self::Item>>
        where
            Self::Error: std::fmt::Display,
            Self::Item: Send,
        Self: Sized + Send + 'static,
    {
        stream_fork::fork_stream(self, degree)
    }

    fn meter(self, name: String) -> probe_stream::Meter<Self>
        where Self: Sized
    {
        probe_stream::Meter::new(self, name)
    }

    fn tag(self) -> probe_stream::Tag<Self>
        where Self: Sized
    {
        probe_stream::Tag::new(self)
    }

    // Chunks
    // from ../src/util.rs
    // map() filer() for_each() ...
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

