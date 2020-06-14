use tokio::prelude::*;
//use tokio;
use futures::try_ready;
#[cfg(stream_profiling)]
use crate::LogHistogram;
#[cfg(stream_profiling)]
use std::time::Instant;

pub struct InstrumentedMap<S, F>
{
    #[cfg(stream_profiling)]
    name: String,
    #[cfg(stream_profiling)]
    hist: LogHistogram,
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
    type Error = S::Error;

    #[cfg(stream_profiling)]
    fn poll(&mut self) -> Poll<Option<U>, S::Error> {
        let option = try_ready!(self.stream.poll());
        let result = match option {
            None => {
                self.hist.print_stats(&self.name);
                None
            },
            Some(item) => {
                let start = Instant::now();
                let result = (self.function)(item);
                self.hist.sample_now(&start);
                Some(result)
            }
        };

        Ok(Async::Ready(result))
    }

    #[cfg(not(stream_profiling))]
    fn poll(&mut self) -> Poll<Option<U>, S::Error> {
        let option = try_ready!(self.stream.poll());
        Ok(Async::Ready(option.map(|item| (self.function)(item) )))
    }
}

pub struct InstrumentedMapChunked<S, F>
{
    #[cfg(stream_profiling)]
    name: String,
    #[cfg(stream_profiling)]
    hist: LogHistogram,
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
    type Error = S::Error;

    #[cfg(stream_profiling)]
    fn poll(&mut self) -> Poll<Option<Self::Item>, S::Error> {
        let option = try_ready!(self.stream.poll());
        let result =  match option {
            None => {
                self.hist.print_stats(&self.name);
                None
            },
            Some(chunk) => {
                let start = Instant::now();
                out_chunk = chunk.into_iter().map(&mut self.function).collect();
                self.hist.sample_now_chunk(out_chunk.len(), &start);
                Some(out_chunk)
            }
        };
        Ok(Async::Ready(result))
    }

    #[cfg(not(stream_profiling))]
    fn poll(&mut self) -> Poll<Option<Self::Item>, S::Error> {
        let option = try_ready!(self.stream.poll());
        let result = option.map(|chunk|
            {
                chunk.into_iter().map(&mut self.function).collect()
            });

        Ok(Async::Ready(result))
    }
}
