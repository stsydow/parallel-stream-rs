use futures::{try_ready, Async, Poll, Stream};
use std::collections::{HashMap, hash_map::Entry};
use std::hash::Hash;

#[cfg(stream_profiling)]
use crate::LogHistogram;
#[cfg(stream_profiling)]
use std::time::Instant;

pub struct SelectiveContext<Key, Ctx, InStream, FInit, FSel, FWork> {
    #[cfg(stream_profiling)]
    name: String,
    #[cfg(stream_profiling)]
    hist: LogHistogram,
    ctx_init: FInit,
    selector: FSel,
    work: FWork,
    context_map: HashMap<Key, Ctx>,
    input: InStream,
}

impl<R, Key, Ctx, InStream, FInit, FSel, FWork>
    SelectiveContext<Key, Ctx, InStream, FInit, FSel, FWork>
where
    Key: Ord + Hash,
    InStream: Stream,
    FInit: Fn(&Key) -> Ctx,
    FSel: Fn(&InStream::Item) -> Key,
    FWork: FnMut(&mut Ctx, InStream::Item) -> R,
{
    pub fn new(input: InStream, ctx_builder: FInit, selector: FSel, work: FWork, name: String) -> Self {
        #[cfg(not(stream_profiling))]
        {let _ = &name;}

        SelectiveContext {
            #[cfg(stream_profiling)]
            name,
            #[cfg(stream_profiling)]
            hist: LogHistogram::new(),
            ctx_init: ctx_builder,
            selector,
            work,
            context_map: HashMap::new(),
            input,
        }
    }

    fn apply(&mut self, event: InStream::Item) -> R {
        let key = (self.selector)(&event);

        //let work_fn = &mut self.work;
        let context = match self.context_map.entry(key) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let inital_ctx = (&self.ctx_init)(entry.key());
                entry.insert(inital_ctx)
            }
        };
        //work_fn(context, event)
        (self.work)(context, event)

        //TODO decide / implement context termination (via work()'s Return Type? An extra function? Timeout registration? )
    }
}

pub fn selective_context<R, Key, Ctx, InStream, CtxInit, FSel, FWork>(
    input: InStream,
    ctx_builder: CtxInit,
    selector: FSel,
    work: FWork,
    name: String
) -> SelectiveContext<Key, Ctx, InStream, CtxInit, FSel, FWork>
where
    InStream: Stream,
    Key: Ord + Hash,
    CtxInit: Fn(&Key) -> Ctx,
    FSel: Fn(&InStream::Item) -> Key,
    FWork: FnMut(&mut Ctx, InStream::Item) -> R,
{
    #[cfg(not(stream_profiling))]
    {let _ = &name;}
    SelectiveContext {
        #[cfg(stream_profiling)]
        name,
        #[cfg(stream_profiling)]
        hist: LogHistogram::new(),
        ctx_init: ctx_builder,
        selector,
        work,
        context_map:HashMap::new(),
        input,
    }
}

impl<R, Key, Ctx, InStream, CtxInit, FSel, FWork> Stream
    for SelectiveContext<Key, Ctx, InStream, CtxInit, FSel, FWork>
where
    InStream: Stream,
    Key: Ord + Hash,
    CtxInit: Fn(&Key) -> Ctx,
    FSel: Fn(&InStream::Item) -> Key,
    FWork: FnMut(&mut Ctx, InStream::Item) -> R,
{
    type Item = R;
    type Error = InStream::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let async_event = try_ready!(self.input.poll());
        let result = match async_event {
            Some(event) => {
                #[cfg(stream_profiling)]
                let start = Instant::now();

                let result = self.apply(event);

                #[cfg(stream_profiling)]
                self.hist.sample_now(&start);

                Some(result)
            },
            None => {
                #[cfg(stream_profiling)]
                self.hist.print_stats(&self.name);

                None
            },
        };

        Ok(Async::Ready(result))
    }
}

pub struct SelectiveContextBuffered<Key, Ctx, InStream, FInit, FSel, FWork>
{
    #[cfg(stream_profiling)]
    name: String,
    #[cfg(stream_profiling)]
    hist: LogHistogram,
    ctx_init: FInit,
    selector: FSel,
    work: FWork,
    context_map:HashMap<Key, Ctx>,
    input:InStream
}


impl<Chunk, R, Key, Ctx, InStream, FInit, FSel, FWork> SelectiveContextBuffered<Key, Ctx, InStream, FInit, FSel, FWork>
    where Chunk: IntoIterator,
          Key: Ord + Hash,
          InStream:Stream<Item=Chunk>,
          FInit: Fn(&Key) -> Ctx,
          FSel: Fn(&Chunk::Item) -> Key,
          FWork:FnMut(&mut Ctx, Chunk::Item) -> R
{
    pub fn new(input:InStream, ctx_builder: FInit, selector: FSel, work: FWork, name: String) -> Self
    {
        #[cfg(not(stream_profiling))]
        {let _ = &name;}

        SelectiveContextBuffered {
            #[cfg(stream_profiling)]
            name,
            #[cfg(stream_profiling)]
            hist: LogHistogram::new(),
            ctx_init: ctx_builder,
            selector,
            work,
            context_map: HashMap::new(),
            input
        }
    }

    fn apply(&mut self, event: Chunk::Item) -> R {
        let key = (self.selector)(&event);

        let work_fn = &mut self.work;
        let context = match self.context_map.entry(key) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let inital_ctx = (&self.ctx_init)(entry.key());
                entry.insert(inital_ctx)
            }
        };
        work_fn(context, event)

        //TODO decide / implement context termination (via work()'s Return Type? An extra function? Timeout registration? )
    }
}

pub fn selective_context_buffered<Chunk, R, Key, Ctx, InStream, CtxInit, FSel, FWork> (input:InStream, ctx_builder: CtxInit, selector: FSel, work: FWork, name: String) -> SelectiveContextBuffered<Key, Ctx, InStream, CtxInit, FSel, FWork>
    where //Ctx:Context<Event=Event, Result=R>,
        Chunk: IntoIterator,
        InStream:Stream<Item=Chunk>,
        Key: Ord + Hash,
        CtxInit:Fn(&Key) -> Ctx,
        FSel: Fn(&Chunk::Item) -> Key,
        FWork:FnMut(&mut Ctx, Chunk::Item) -> R
{
    #[cfg(not(stream_profiling))]
    {let _ = &name;}
    SelectiveContextBuffered {
        #[cfg(stream_profiling)]
        name,
        #[cfg(stream_profiling)]
        hist: LogHistogram::new(),
        ctx_init: ctx_builder,
        selector,
        work,
        context_map: HashMap::new(),
        input
    }
}


impl<Chunk, Error, R, Key, Ctx, InStream, CtxInit, FSel, FWork> Stream for SelectiveContextBuffered<Key, Ctx, InStream, CtxInit, FSel, FWork>
    where //Ctx:Context<Event=Event, Result=R>,
        Chunk: IntoIterator,
        InStream: Stream<Item=Chunk, Error=Error>,
        Key: Ord + Hash,
        CtxInit:Fn(&Key) -> Ctx,
        FSel: Fn(&Chunk::Item) -> Key,
        FWork:FnMut(&mut Ctx, Chunk::Item) -> R
{
    type Item = Vec<R>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let async_chunk = try_ready!(self.input.poll());

        let result = match async_chunk {
            Some(chunk)=> {

                #[cfg(stream_profiling)]
                let start = Instant::now();

                let result_chunk = chunk.into_iter().map(|e| self.apply(e)).collect();
                /*
                let mut result_chunk = Vec::with_capacity(chunk.len());

                for event in chunk {
                    let out_ev = self.apply(event);
                    result_chunk.push(out_ev);
                }
                */

                #[cfg(stream_profiling)]
                self.hist.sample_now_chunk(chunk.len(), &start);

                Some(result_chunk)
            },
            None => {
                #[cfg(stream_profiling)]
                self.hist.print_stats(&self.name);
                None
            }
        };

        Ok(Async::Ready(result))
    }
}
