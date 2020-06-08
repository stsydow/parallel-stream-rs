use crate::instrumented_map::{self, InstrumentedMapChunked, InstrumentedMap};
use crate::instrumented_fold::{self, InstrumentedFold};
use crate::selective_context::{self, SelectiveContext, SelectiveContextBuffered};
use crate::probe_stream;
use crate::stream_fork;
use crate::{Tag, ParallelStream};
use tokio::prelude::*;

use std::hash::Hash;

impl<T: ?Sized> StreamExt for T where T: Stream {}

pub trait StreamExt: Stream {
    // Instrumentation
    fn instrumented_map<U, F>(self, f: F, name: String) -> InstrumentedMap<Self, F>
        where F: FnMut(Self::Item) -> U,
              Self: Sized
    {
        instrumented_map::instrumented_map(self, f, name)
    }

    fn instrumented_map_chunked<I, U, F>(self, f: F, name:String) -> InstrumentedMapChunked<Self, F>
        where Self:Stream<Item=Vec<I>>,
              F: FnMut(I) -> U,
              Self: Sized
    {
        instrumented_map::instrumented_map_chunked(self, f, name)
    }

    fn instrumented_fold<Fut, T, F>(self, init:T, f:F, name: String) -> InstrumentedFold<Self, F, Fut, T>
        where F: FnMut(T, Self::Item) -> Fut,
              Fut: IntoFuture<Item = T>,
              Self::Error: From<Fut::Error>,
              Self: Sized,

    {
        instrumented_fold::instrumented_fold(self, f, init, name)
    }


    //TODO instrument ; add name
    fn selective_context<R, Key, Ctx, CtxInit, FSel, FWork>(
        self,
        ctx_builder: CtxInit,
        selector: FSel,
        work: FWork,
        name: String
    ) -> SelectiveContext<Key, Ctx, Self, CtxInit, FSel, FWork>
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
    ) -> SelectiveContextBuffered<Key, Ctx, Self, CtxInit, FSel, FWork>
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

    fn fork(self, degree: usize) -> ParallelStream<tokio::sync::mpsc::Receiver<Tag<Self::Item>>>
        where
            Self::Item: Send,
            Self: Sized + Send + 'static,
    {
        stream_fork::fork_stream(self, degree)
    }

    fn fork_sel<FSel>(self, selector: FSel, degree: usize) -> Vec<tokio::sync::mpsc::Receiver<Self::Item>>
        where
            Self::Item: Send,
            FSel: Fn(&Self::Item) -> usize,
            Self: Sized + Send + 'static,
    {
        unimplemented!();
        //stream_fork::fork_stream_sel(self, selector, degree)
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
