use crate::instrumented_map::{self, InstrumentedMapChunked, InstrumentedMap};
use crate::instrumented_fold::{self, InstrumentedFold};
use crate::selective_context::{self, SelectiveContext, SelectiveContextBuffered};
use crate::probe_stream;
use crate::stream_fork;
use crate::stream_fork_chunked;
use crate::tagged_stream;
use crate::{TaggedStream, ParallelStream, Receiver, channel};

use futures::prelude::*;
use tokio::runtime::Runtime;

use std::hash::Hash;

impl<T: ?Sized> StreamExt for T where T: Stream {}

pub trait StreamExt: Stream {
    fn instrumented_map<U, F>(self, f: F, name: String) -> InstrumentedMap<Self, F>
        where F: FnMut(Self::Item) -> U,
              Self: Sized
    {
        instrumented_map::instrumented_map(self, f, name)
    }

    fn instrumented_fold<Fut, T, F>(self, init:T, f:F, name: String) -> InstrumentedFold<Self, F, Fut, T>
        where F: FnMut(T, Self::Item) -> Fut,
              Fut: Future<Output = T>,
              Self: Sized,

    {
        instrumented_fold::instrumented_fold(self, f, init, name)
    }

    fn selective_context<R, Key, Ctx, CtxInit, FSel, FWork>(
        self,
        ctx_builder: CtxInit,
        selector: FSel,
        work: FWork,
        name: String
    ) -> SelectiveContext<Key, Ctx, Self, CtxInit, FSel, FWork>
    where
        Key: Ord + Hash,
        CtxInit: Fn(&Key) -> Ctx,
        FSel: Fn(&Self::Item) -> Key,
        FWork: FnMut(&mut Ctx, Self::Item) -> R,
        Self: Sized,
    {
        selective_context::selective_context(self, ctx_builder, selector, work, name)
    }

    fn tagged(self) -> TaggedStream<Self>
        where Self: Sized
    {
        tagged_stream::tagged_stream(self)
    }

    fn fork(self, degree: usize, buf_size: usize, exec: &Runtime) -> ParallelStream<crate::channel::Receiver<Self::Item>>
        where
            Self::Item: Send,
            Self: Sized + Send + 'static,
    {
        stream_fork::fork_stream(self, degree, buf_size, exec)
    }

    fn fork_sel<FSel>(self, selector: FSel, degree: usize, buf_size: usize, exec: &Runtime) -> ParallelStream<crate::channel::Receiver<Self::Item>>
        where
            Self::Item: Send,
            FSel: Fn(&Self::Item) -> usize + Copy + Send + 'static,
            Self: Sized + Send + 'static,
    {
        stream_fork::fork_stream_sel(self, selector, degree, buf_size, exec)
    }

    fn forward_and_spawn<SOut:Sink<Self::Item>>(self, sink:SOut, exec: &Runtime) -> tokio::task::JoinHandle<std::result::Result<(), SOut::Error>>
        where
            SOut: Send + 'static,
            SOut::Error: Send + std::fmt::Debug,
            Self::Item: Send,
            Self: Sized + Send + 'static,
    {
        use futures::TryFutureExt;

        let task = crate::forward::Forward::new(self, sink)
        //let task = self.forward(sink)
            .map_err(|e| {
                panic!("send error:{:#?}", e)
            });

        exec.spawn(task)
    }

    fn forward<SOut:Sink<Self::Item>>(self, sink:SOut) -> crate::forward::Forward<Self, SOut, Self::Item>
        where Self: Sized,
    {
        crate::forward::Forward::new(self, sink)
    }

    fn decouple(self, buf_size: usize, exec: &Runtime) -> Receiver<Self::Item>
        where Self::Item: Send,
            Self: Sized + Send + 'static,
    {
        let (tx, rx) = channel::<Self::Item>(buf_size);

        self.forward_and_spawn(tx, exec);

        rx
    }

    fn meter(self, name: String) -> probe_stream::Meter<Self>
        where Self: Sized
    {
        probe_stream::Meter::new(self, name)
    }

    fn time_tagged(self) -> probe_stream::Tag<Self>
        where Self: Sized
    {
        probe_stream::Tag::new(self)
    }

    // map() filer() for_each() ...
}

impl<T: ?Sized, Chunk: IntoIterator> StreamChunkedExt for T where T: Stream<Item=Chunk>
{}

pub trait StreamChunkedExt: Stream {
    fn instrumented_map_chunked<U, F, C>(self, f: F, name:String) -> InstrumentedMapChunked<Self, F>
        where C: IntoIterator,
              Self:Stream<Item=C>,
              F: FnMut( C::Item) -> U,
              Self: Sized ,
    {
        instrumented_map::instrumented_map_chunked(self, f, name)
    }

    fn selective_context_buffered<Chunk, R, Key, Ctx, CtxInit, FSel, FWork>(
        self,
        ctx_builder: CtxInit,
        selector: FSel,
        work: FWork,
        name: String
    ) -> SelectiveContextBuffered<Key, Ctx, Self, CtxInit, FSel, FWork>
    where
        Chunk: IntoIterator,
        Self:Stream<Item=Chunk>,
        Key: Ord + Hash,
        CtxInit: Fn(&Key) -> Ctx,
        FSel: Fn(&Chunk::Item) -> Key,
        FWork: FnMut(&mut Ctx, Chunk::Item) -> R,
        Self: Sized,
    {
        selective_context::selective_context_buffered(self, ctx_builder, selector, work, name)
    }

    fn fork_sel_chunked<FSel, Chunk>(self, selector: FSel, degree: usize, buf_size: usize, exec: &Runtime) -> ParallelStream<crate::channel::Receiver<Vec<Chunk::Item>>>
        where
            Chunk: IntoIterator,
            Chunk::Item: Send + 'static,
            FSel: Fn(&Chunk::Item) -> usize + Copy + Send + 'static,
            Self: Stream<Item=Chunk> + Sized + Send + 'static,
    {
        stream_fork_chunked::fork_stream_sel_chunked(self, selector, degree, buf_size, exec)
    }
}
