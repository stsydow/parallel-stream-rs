use futures::prelude::*;
use futures::task::Poll;
use futures::ready;
use std::pin::Pin;
use core::task::Context;
use pin_project::pin_project;
#[cfg(stream_profiling)]
use crate::LogHistogram;
#[cfg(stream_profiling)]
use std::time::Instant;

#[pin_project(project = InstrumentedMapProj)]
pub struct InstrumentedMap<S, F>
{
    #[cfg(stream_profiling)]
    name: String,
    #[cfg(stream_profiling)]
    hist: LogHistogram,
#[pin]
    stream: S,
    function: F
}

pub fn instrumented_map<S, F, U>(stream: S, function: F, name: String) -> InstrumentedMap<S, F>
    where S: Stream,
          F: FnMut(S::Item) -> U,
{
    #[cfg(not(stream_profiling))]
    {let _ = &name;}

    InstrumentedMap {
        #[cfg(stream_profiling)]
        name,
        #[cfg(stream_profiling)]
        hist: LogHistogram::new(),
        stream,
        function,
    }
}

impl<S, I, F, U> Stream for InstrumentedMap<S, F>
    where S: Stream<Item=I>,
          F: FnMut(S::Item) -> U,
{
    type Item = U;

    #[cfg(stream_profiling)]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<U>>
    {
        let InstrumentedMapProj { name, hist, stream, function } = self.project();
        let option = ready!(stream.poll_next(cx));
        let result = match option {
            None => {
                hist.print_stats(&name);
                None
            },
            Some(item) => {
                let start = Instant::now();
                let result = (function)(item);
                hist.sample_now(&start);
                Some(result)
            }
        };

        Poll::Ready(result)
    }

    #[cfg(not(stream_profiling))]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<U>>
    {
        let InstrumentedMapProj { stream, function } = self.project();
        let option = ready!(stream.poll_next(cx));
        Poll::Ready(option.map(|item| (function)(item) ))
    }
}

#[pin_project(project = InstrumentedMapChunkedProj)]
pub struct InstrumentedMapChunked<S, F>
{
    #[cfg(stream_profiling)]
    name: String,
    #[cfg(stream_profiling)]
    hist: LogHistogram,
#[pin]
    stream: S,
    function: F
}

pub fn instrumented_map_chunked<S, C, F, U>(stream: S, function: F, name: String) -> InstrumentedMapChunked<S, F>
    where C: IntoIterator,
          S: Stream<Item=C>,
          F: FnMut(C::Item) -> U,
{
    #[cfg(not(stream_profiling))]
    {let _ = &name;}

    InstrumentedMapChunked {
        #[cfg(stream_profiling)]
        name,
        #[cfg(stream_profiling)]
        hist: LogHistogram::new(),
        stream,
        function,
    }
}

impl<S, C, F, U> Stream for InstrumentedMapChunked<S, F>
    where C: IntoIterator,
          S: Stream<Item=C>,
          F: FnMut(C::Item) -> U,
{
    type Item = Vec<U>;
    //type Item = std::iter::Map<C::IntoIter, &'static mut F>;
    //type Item = std::iter::Map<C::IntoIter, F>;

    #[cfg(stream_profiling)]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>>
    {
        let InstrumentedMapChunkedProj { name, hist, stream, mut function } = self.project();
        let option = ready!(stream.poll_next(cx));
        let result =  match option {
            None => {
                hist.print_stats(&name);
                None
            },
            Some(chunk) => {
                let start = Instant::now();
                let out_chunk:Self::Item = chunk.into_iter().map(&mut function).collect();
                hist.sample_now_chunk(out_chunk.len(), &start);
                Some(out_chunk)
            }
        };
        Poll::Ready(result)
    }

    #[cfg(not(stream_profiling))]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>>
    {
        let InstrumentedMapChunkedProj { stream, mut function } = self.project();
        let option = ready!(stream.poll_next(cx));
        let result = option.map(|chunk|
            {
                chunk.into_iter().map(&mut function).collect()
            });

        Poll::Ready(result)
    }
}
