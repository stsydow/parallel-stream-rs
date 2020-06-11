
use std::hash::Hash;
use tokio::prelude::*;
use tokio::sync::mpsc::{Receiver, channel};

use crate::{StreamExt, Tag, JoinTagged, SelectiveContext, SelectiveContextBuffered, InstrumentedMap, InstrumentedMapChunked, InstrumentedFold};
use crate::stream_join::join_tagged;
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

impl<S> ParallelStream<S>
{
    pub fn width(&self) -> usize {
        self.streams.len()
    }
}

impl<S, I> ParallelStream<S>
where
    //I: Send + 'static,
    S: Stream<Item=Tag<I>>,// + Send + 'static,
{
    /*
    pub fn width(&self) -> usize {
        self.streams.len()
    }
    */

    pub fn join_ordered(self) -> JoinTagged<S> {
        join_tagged(self)
    }

    pub fn instrumented_map_tagged<U, F, FTag>(self, f: F, name:String)
        -> ParallelStream<InstrumentedMap<S, impl FnMut(Tag<I>) -> Tag<U> >>
        where F: FnMut(I) -> U + Copy,
              //FTag: FnMut(Tag<I>) -> Tag<U>
    {
        let mut streams = Vec::new();
        for input in self.streams {
            //let map = input.instrumented_map(map_tag(f), name.clone());
            let map = input.instrumented_map(move |t| t.map(f) , name.clone());
            streams.push(map);
        }
        ParallelStream{ streams }
    }


    pub fn untag(self) -> ParallelStream<stream::Map<S, impl FnMut(S::Item) -> I>> {
        let mut streams = Vec::new();
        for input in self.streams {
            let map = input.map(|t| t.untag());
            streams.push(map);
        }
        ParallelStream{ streams }

    }

    //TODO map chunkedmap fold
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

    pub fn shuffle_tagged<FSel>(self, selector: FSel, degree: usize) ->
        // TODO is: ParallelStream<JoinTagged<Receiver<Tag<Tag<I>>>>>
        // TODO really should be:
        ParallelStream<JoinTagged<Receiver<Tag<I>>>>
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
            let splits = input.fork_sel( move |t| (selector)(t.as_ref()), degree);
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

    pub fn decouple(self, buf_size: usize) ->  ParallelStream<Receiver<S::Item>> {
        self.add_stage(|s| s.decouple(buf_size))
    }

    pub fn join_unordered(self, buf_size: usize) ->  Receiver<S::Item> {

        let (join_tx, join_rx) = channel::<S::Item>(buf_size);
        for input in self.streams {
            let pipe_task = input
                .forward(join_tx.clone().sink_map_err(|e| {
                    eprintln!("join in send error:{}", e);
                    panic!()
                }))
                .and_then(|(_in, tx)|  tx.flush())
                .map( |_tx| () )
                .map_err(|_e| {
                    panic!()
                });

            tokio::spawn(pipe_task);
        }

        join_rx
    }

    pub fn shuffle_unordered<FSel>(self, selector: FSel, degree: usize) ->
        // TODO is: ParallelStream<JoinTagged<Receiver<Tag<Tag<I>>>>>
        // TODO really should be:
        ParallelStream<Receiver<S::Item>>
    where
        S: Send + 'static,
        FSel: Fn(&S::Item) -> usize + Copy + Send + 'static,
    {
        let input_degree = self.streams.len();
        //let joins_streams = vec![ Vec::with_capacity(input_degree); degree];
        let mut joins_streams = Vec::with_capacity(degree);
        for _i in 0 .. degree {
            joins_streams.push( Vec::with_capacity(input_degree))
        }

        for input in self.streams {
            let splits = input.fork_sel(selector, degree);
            // TODO here it gets a other Tag


            let mut i = 0;
            for s in splits.streams {
                joins_streams[i].push(s);
                i += 1;
            }
        }

        let joins: Vec<Receiver<S::Item>> = joins_streams.into_iter().map( |join_st| ParallelStream::from(join_st).join_unordered(input_degree)).collect();

        ParallelStream::from(joins)
    }
}

impl<S: Stream> ParallelStream<S> //TODO this is untagged -- we need a tagged version
{

    pub fn add_stage<U, F>(self, stage: F) -> ParallelStream<U>
        where F: FnMut(S) -> U,
    {
        let streams =  self.streams.into_iter().map(stage).collect();
        ParallelStream{ streams }
    }

    pub fn instrumented_map<U, F>(self, f: F, name:String)
        -> ParallelStream<InstrumentedMap<S, F>>
        where F: FnMut(S::Item) -> U + Copy,
    {
        self.add_stage(|s| s.instrumented_map(f, name.clone()))
    }

    pub fn instrumented_fold<Fut, T, F, Finit>(self, init: Finit, f:F, name: String) ->
        impl Stream<Item=T, Error=S::Error>
        //futures::stream::BufferUnordered< futures::stream::IterOk< ... InstrumentedFold<S, F, Fut, T>, S::Error>>
        where S: 'static,
              Finit: Fn() -> T + Copy + 'static,
              F: FnMut(T, S::Item) -> Fut + Copy + 'static,
              Fut: IntoFuture<Item = T>,
              S::Error: From<Fut::Error>,
              //Self: Sized,

    {
        let degree = self.streams.len();
        futures::stream::iter_ok(self.streams).map(move |input|{
            input.instrumented_fold(init(), f, name.clone())
        }).buffer_unordered(degree)
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
        FWork: Fn(&mut Ctx, &S::Item) -> R + Copy,
    {
        self.add_stage(|s| s.selective_context(ctx_builder, selector, work, name.clone()))
    }
}

impl<S: Stream, I> ParallelStream<S>
    where S:Stream<Item=Vec<I>>,
{

    pub fn instrumented_map_chunked<U, F>(self, f: F, name:String)
        -> ParallelStream<InstrumentedMapChunked<S, F>>
        where F: FnMut(I) -> U + Copy,
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
        FSel: Fn(&I) -> Key + Copy,
        FWork: Fn(&mut Ctx, &I) -> R + Copy,
        S: Sized,
    {
        self.add_stage(|s| s.selective_context_buffered(ctx_builder, selector, work, name.clone()))
    }
}
