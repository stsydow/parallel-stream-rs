use futures::prelude::*;
use std::hash::Hash;

use crate::{Receiver, channel::Sender, channel};
use tokio::runtime::Runtime;
use crate::{StreamExt, StreamChunkedExt, Tag, JoinTagged, SelectiveContext, SelectiveContextBuffered, InstrumentedMap, InstrumentedMapChunked, InstrumentedFold};
use crate::stream_join::join_tagged;
use crate::stream_fork::fork_sel;
//TODO see https://github.com/async-rs/parallel-stream/

pub struct ParallelStream<S>
{
    streams: Vec<S>,
}

impl<S> From<ParallelStream<S>> for Vec<S> {
    fn from(p: ParallelStream<S>) -> Vec<S> {
        p.streams
    }
}

impl<S> From<Vec<S>> for ParallelStream<S> {
    fn from(streams: Vec<S>) -> ParallelStream<S> {
        ParallelStream{streams}
    }
}

impl<S> IntoIterator for ParallelStream<S> {
    type Item = S;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.streams.into_iter()
    }
}

impl<S> ParallelStream<S>
{
    pub fn width(&self) -> usize {
        self.streams.len()
    }

    pub fn add_stage<U, F>(self, stage: F) -> ParallelStream<U>
        where F: FnMut(S) -> U,
    {
        let streams =  self.streams.into_iter().map(stage).collect();
        ParallelStream{ streams }
    }
}

impl<S, I> ParallelStream<S>
where
    S: Stream<Item=Tag<I>>,
{
    pub fn join_ordered(self) -> JoinTagged<S> {
        join_tagged(self)
    }

    pub fn instrumented_map_tagged<U, F, FTag>(self, f: F, name:String)
        -> ParallelStream<InstrumentedMap<S, impl FnMut(Tag<I>) -> Tag<U> >>
        where F: Fn(I) -> U + Copy,
              //FTag: FnMut(Tag<I>) -> Tag<U>
    {
        self.add_stage(|s| s.instrumented_map(move |t| t.map(f) , name.clone()))
    }


    pub fn untag(self) -> ParallelStream<stream::Map<S, impl FnMut(S::Item) -> I>> {
        self.add_stage(|s| s.map(|t| t.untag()))
    }

    pub fn shuffle_tagged<FSel>(self, selector: FSel, degree: usize, buf_size: usize, exec: &Runtime) ->
        ParallelStream<JoinTagged<Receiver<Tag<I>>>>
    where
        I: Send,
        S: Send + 'static,
        FSel: Fn(&I) -> usize + Copy + Send + 'static,
    {
        let input_degree = self.streams.len();
        let mut joins_streams = Vec::with_capacity(degree);
        for _i in 0 .. degree {
            joins_streams.push( Vec::with_capacity(input_degree))
        }

        for input in self.streams {
            let splits = input.fork_sel( move |t| (selector)(t.as_ref()), degree, buf_size, exec);
            // TODO here it gets a other Tag


            let mut i = 0;
            for s in splits.streams {
                joins_streams[i].push(s);
                i += 1;
            }
        }

        let mut joins = Vec::with_capacity(degree);
        for streams in joins_streams {
            joins.push(ParallelStream{streams}.join_ordered()) //TODO here it is removed

        }

        ParallelStream::from(joins)
    }
}

impl<S> ParallelStream<S>
where
    S: Stream + Send + 'static,
    S::Item: Send + 'static,
{

    pub fn decouple(self, buf_size: usize, exec: &Runtime) ->  ParallelStream<Receiver<S::Item>> {
        self.add_stage(|s| s.decouple(buf_size, exec))
    }

    pub fn join_unordered(self, buf_size: usize, exec: &Runtime) ->  Receiver<S::Item> {

        let (join_tx, join_rx) = channel::<S::Item>(buf_size);
        for input in self.streams {
            input.forward_and_spawn(join_tx.clone(), exec);
        }

        join_rx
    }

    pub fn shuffle_unordered<FSel>(self, selector: FSel, degree: usize, buf_size: usize, exec: &Runtime) ->
        ParallelStream<Receiver<S::Item>>
    where
        S: Send + 'static,
        FSel: Fn(&S::Item) -> usize + Copy + Send + 'static,
    {
        let mut joins_streams = Vec::with_capacity(degree);
        let mut joins_sinks = Vec::with_capacity(degree);
        for _i in 0 .. degree {
            let (tx, rx) = channel::<S::Item>(buf_size);
            joins_streams.push(rx);
            joins_sinks.push(tx)
        }

        for stream in self.streams {
            let sinks:Vec<Sender<S::Item>> = joins_sinks.iter().map(|s| s.clone()).collect();
            let fork = fork_sel(sinks, selector);
            stream.forward_and_spawn(fork, exec);
        }

        ParallelStream::from(joins_streams)
    }
}

impl<S: Stream> ParallelStream<S> //TODO this is untagged -- we need a tagged version
{

    pub fn map<U, F>(self, f: F)
        -> ParallelStream<futures::stream::Map<S, F>>
        where F: Fn(S::Item) -> U + Copy,
    {
        self.add_stage(|s| s.map(f))
    }

    pub fn instrumented_map<U, F>(self, f: F, name:String)
        -> ParallelStream<InstrumentedMap<S, F>>
        where F: Fn(S::Item) -> U + Copy,
    {
        self.add_stage(|s| s.instrumented_map(f, name.clone()))
    }

    pub fn instrumented_fold<Fut, T, F, Finit>(self, init: Finit, f:F, name: String) ->
        ParallelStream<InstrumentedFold<S, F, Fut, T>>
        where Finit: Fn() -> T + Copy,
              F: Fn(T, S::Item) -> Fut + Copy,
              Fut: Future<Output = T>,

    {
        self.add_stage(|s| s.instrumented_fold(init(), f, name.clone()))
    }

    pub fn fold<Fut, T, F, Finit>(self, init: Finit, f:F) ->
        ParallelStream<futures::stream::Fold<S, Fut, T, F>>
        where Finit: Fn() -> T + Copy,
              F: Fn(T, S::Item) -> Fut + Copy,
              Fut: Future<Output = T>,

    {
        self.add_stage(|s| s.fold(init(), f))
    }

    pub fn selective_context<R, Key, Ctx, CtxInit, FSel, FWork>(
        self,
        ctx_builder: CtxInit,
        selector: FSel,
        work: FWork,
        name: String
    ) -> ParallelStream<SelectiveContext<Key, Ctx, S, CtxInit, FSel, FWork>>
    where
        Key: Ord + Hash,
        CtxInit: Fn(&Key) -> Ctx + Copy,
        FSel: Fn(&S::Item) -> Key + Copy,
        FWork: Fn(&mut Ctx, S::Item) -> R + Copy,
    {
        self.add_stage(|s| s.selective_context(ctx_builder, selector, work, name.clone()))
    }
}

impl<R: Future> ParallelStream<R>
{

    pub fn map_result<U, F>(self, f: F)
        -> ParallelStream<futures::future::Map<R, F>>
        where F: Fn(R::Output) -> U + Copy,
    {
        self.add_stage(|s| s.map(f))
    }

    pub fn flatten_stream(self) -> ParallelStream<futures::future::FlattenStream<R>>
        where R::Output: Stream,
    {
        self.add_stage(|s| s.flatten_stream())
    }

    pub fn merge<Fut, T, F>(self, init: T, f:F, exec: &Runtime) ->
        futures::stream::Fold<Receiver<R::Output>, Fut, T, F>
        where F: FnMut(T, R::Output) -> Fut,
              Fut: Future<Output = T>,
              R::Output: Send + 'static,
              R: Send + 'static,

    {
        let (join_tx, join_rx) = channel::<R::Output>(self.width());
        for input in self.streams {
            let mut local_sender = join_tx.clone();
            let task = input.then(move |result| async move {
                let r = local_sender.send(result).await;
                r.expect("send error - receiver already closed")
            });
            let _task_handle = exec.spawn(Box::pin(task));
        }

        join_rx.fold(init, f)
    }
}

impl<S: Stream, C> ParallelStream<S>
    where  C: IntoIterator,
          S:Stream<Item=C>,
{

    pub fn instrumented_map_chunked<U, F>(self, f: F, name:String)
        -> ParallelStream<InstrumentedMapChunked<S, F>>
        where F: Fn(C::Item) -> U + Copy,
    {
        self.add_stage(|s| s.instrumented_map_chunked(f, name.clone()))
    }

    // TODO only useful after shuffle
    pub fn selective_context_buffered<R, Key, Ctx, CtxInit, FSel, FWork>(
        self,
        ctx_builder: CtxInit,
        selector: FSel,
        work: FWork,
        name: String
    ) -> ParallelStream<SelectiveContextBuffered<Key, Ctx, S, CtxInit, FSel, FWork>>
    where
        Key: Ord + Hash,
        CtxInit: Fn(&Key) -> Ctx + Copy,
        FSel: Fn(&C::Item) -> Key + Copy,
        FWork: Fn(&mut Ctx, C::Item) -> R + Copy,
        S: Sized,
    {
        self.add_stage(|s| s.selective_context_buffered(ctx_builder, selector, work, name.clone()))
    }

    /* TODO
    pub fn instrumented_fold_chunked<Fut, T, F, Finit>(self, init: Finit, f:F, name: String) ->
        impl Stream<Item=T, Error=S::Error>
        //futures::stream::BufferUnordered< futures::stream::IterOk< ... InstrumentedFold<S, F, Fut, T>, S::Error>>
        where S: 'static,
              Finit: Fn() -> T + Copy + 'static,
              F: FnMut(T, C::Item) -> Fut + Copy + 'static,
              Fut: Future<Item = T>,
              S::Error: From<Fut::Error>,
              //Self: Sized,

    {
        let degree = self.streams.len();
        futures::stream::iter_ok(self.streams).map(move |input|{
            unimplemented!();
            //input.instrumented_fold_chunked(init(), f, name.clone())
        }).buffer_unordered(degree)
    }
    */

    pub fn shuffle_unordered_chunked<FSel>(self, selector: FSel, degree: usize, buf_size: usize, exec: &Runtime) ->
        ParallelStream<Receiver<Vec<C::Item>>>
    where
        C::Item: Send + 'static,
        S: Send + 'static,
        FSel: Fn(&C::Item) -> usize + Copy + Send + 'static,
    {
        let input_degree = self.streams.len();
        let mut joins_streams = Vec::with_capacity(degree);
        for _i in 0 .. degree {
            joins_streams.push( Vec::with_capacity(input_degree))
        }

        for input in self.streams {
            let splits = input.fork_sel_chunked(selector, degree, buf_size, exec);

            let mut i = 0;
            for s in splits.streams {
                joins_streams[i].push(s);
                i += 1;
            }
        }

        let joins: Vec<Receiver<Vec<C::Item>>> = joins_streams.into_iter()
            .map( |join_st| ParallelStream::from(join_st)
            .join_unordered(input_degree, exec))
            .collect();

        ParallelStream::from(joins)
    }
}
