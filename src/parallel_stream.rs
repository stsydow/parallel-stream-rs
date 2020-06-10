
use std::cmp::Ordering;
use std::hash::Hash;
use tokio::prelude::*;
use tokio::sync::mpsc::{Receiver, channel};

use crate::{StreamExt, JoinTagged, SelectiveContext, SelectiveContextBuffered, InstrumentedMap, InstrumentedMapChunked};
use crate::stream_join::join_tagged;
//TODO see https://github.com/async-rs/parallel-stream/

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

    pub fn nr(&self) -> usize { self.seq_nr }

    pub fn map<F, U>(self, mut f:F) -> Tag<U>
        where F:FnMut(I) -> U
    {
        tag(self.seq_nr, (f)(self.data))
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

impl<S, I> ParallelStream<S>
where
    //I: Send + 'static,
    S: Stream<Item=Tag<I>>,// + Send + 'static,
{
    pub fn width(&self) -> usize {
        self.streams.len()
    }

    pub fn join(self) -> JoinTagged<S, I> {
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


    pub fn untag(self) -> ParallelStream<impl Stream<Item=I, Error=S::Error>> {
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

    pub fn shuffle<FSel>(self, selector: FSel, degree: usize) ->
        //ParallelStream<impl Stream<Item=Tag<I>>>
        //ParallelStream<impl Stream<Item=Tag<I>, Error=RecvError<S::Error>>>
        ParallelStream<JoinTagged<Receiver<S>>>
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

impl<S> ParallelStream<S>
where
    S: Stream + Send + 'static,
    S::Item: Send + 'static,
{

    pub fn decouple(self, buf_size: usize) ->  ParallelStream<Receiver<S::Item>> {

        let mut streams = Vec::new();
        for input in self.streams {
            let (tx, rx) = channel::<S::Item>(buf_size);
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

    pub fn instrumented_map<U, F>(self, f: F, name:String)
        -> ParallelStream<InstrumentedMap<S, F>>
        where F: FnMut(S::Item) -> U + Copy,
    {
        let mut streams = Vec::new();
        for input in self.streams {
            let map = input.instrumented_map(f, name.clone());
            streams.push(map);
        }
        ParallelStream{ streams }
    }

    pub fn instrumented_fold<Fut, T, F, Finit>(self, init: Finit, f:F, name: String) ->
        impl Stream<Item=T, Error=S::Error>
        //ParallelTask<InstrumentedFold<S, F, Fut, T>>
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
        let mut streams = Vec::new();
        for input in self.streams {
            let ctx = input.selective_context(ctx_builder, selector, work, name.clone());
            streams.push(ctx);
        }
        ParallelStream{ streams }
    }
}

impl<S: Stream, I> ParallelStream<S>
    where S:Stream<Item=Vec<I>>,
{

    pub fn instrumented_map_chunked<U, F>(self, f: F, name:String)
        -> ParallelStream<InstrumentedMapChunked<S, F>>
        where F: FnMut(I) -> U + Copy,
    {
        let mut streams = Vec::new();
        for input in self.streams {
            let map = input.instrumented_map_chunked(f, name.clone());
            streams.push(map);
        }
        ParallelStream{ streams }
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
        //Ctx:Context<Event=Event, Result=R>,
        Key: Ord + Hash,
        CtxInit: Fn(&Key) -> Ctx + Copy,
        FSel: Fn(&I) -> Key + Copy,
        FWork: Fn(&mut Ctx, &I) -> R + Copy,
        S: Sized,
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
