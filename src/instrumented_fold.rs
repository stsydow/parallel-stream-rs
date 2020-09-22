#[cfg(stream_profiling)]
use std::time::Instant;
#[cfg(stream_profiling)]
use crate::LogHistogram;
use core::mem;
use futures::task::{Poll, Context};
use std::pin::Pin;
use futures::prelude::*;
use std::future::IntoFuture;

#[derive(Debug)]
#[must_use = "streams do nothing unless polled"]
pub struct InstrumentedFold<S, F, Fut, T> where Fut: IntoFuture
{
    #[cfg(stream_profiling)]
    name: String,
    #[cfg(stream_profiling)]
    hist: LogHistogram,
    stream: S,
    f: F,
    state: State<T, Fut::Future>,
}

#[derive(Debug)]
enum State<T, F> where F: Future {
    /// Placeholder state when doing work
    Empty,

    /// Ready to process the next stream item; current accumulator is the `T`
    Ready(T),

    /// Working on a future the process the previous stream item
    #[cfg(stream_profiling)]
    Processing((Instant, F)),
    #[cfg(not(stream_profiling))]
    Processing(F),
}

pub fn instrumented_fold<S, F, Fut, T>(s: S, f: F, t: T, name: String) -> InstrumentedFold<S, F, Fut, T>
    where S: Stream,
          F: FnMut(T, S::Item) -> Fut,
          Fut: IntoFuture<Output = T>,
{
    #[cfg(not(stream_profiling))]
    {let _ = &name;}

    InstrumentedFold {
        #[cfg(stream_profiling)]
        name,
        #[cfg(stream_profiling)]
        hist: LogHistogram::new(),
        stream: s,
        f: f,
        state: State::Ready(t),
    }
}

impl<S, F, Fut, T> Future for InstrumentedFold<S, F, Fut, T>
    where S: Stream,
          F: FnMut(T, S::Item) -> Fut,
          Fut: IntoFuture<Output = T>,
{
    type Output = T;

    #[cfg(stream_profiling)]
    fn poll(&mut self) -> Poll<T> {
        loop {
            match mem::replace(&mut self.state, State::Empty) {
                State::Empty => panic!("cannot poll Fold twice"),
                State::Ready(state) => {
                    match self.stream.poll()? {
                        Poll::Ready(Some(e)) => {
                            let start_time = Instant::now();
                            let future = (self.f)(state, e);
                            let future = future.into_future();
                            self.state = State::Processing((start_time, future)); //TODO: we messure scheduling here!
                        }
                        Poll::Ready(None) => {
                            self.hist.print_stats(&self.name);
                            return Ok(Async::Ready(state));
                        },
                        Poll::Pending => {
                            self.state = State::Ready(state);
                            return Ok(Async::NotReady)
                        }
                    }
                }
                State::Processing((start_time, mut fut)) => {
                    match fut.poll()? {
                        Poll::Ready(state) => {
                            self.hist.sample_now(&start_time);
                            self.state = State::Ready(state)
                        },
                        Poll::Pending => {
                            self.state = State::Processing((start_time, fut));
                            return Ok(Async::NotReady)
                        }
                    }
                }
            }
        }
    }

    #[cfg(not(stream_profiling))]
    fn poll(&mut self) -> Poll<T> {
        loop {
            match mem::replace(&mut self.state, State::Empty) {
                State::Empty => panic!("cannot poll Fold twice"),
                State::Ready(state) => {
                    match self.stream.poll()? {
                        Async::Ready(Some(e)) => {
                            let future = (self.f)(state, e);
                            let future = future.into_future();
                            self.state = State::Processing(future);
                        }
                        Async::Ready(None) => {
                            return Ok(Async::Ready(state));
                        },
                        Async::NotReady => {
                            self.state = State::Ready(state);
                            return Ok(Async::NotReady)
                        }
                    }
                }
                State::Processing(mut fut) => {
                    match fut.poll()? {
                        Async::Ready(state) => {
                            self.state = State::Ready(state)
                        },
                        Async::NotReady => {
                            self.state = State::Processing(fut);
                            return Ok(Async::NotReady)
                        }
                    }
                }
            }
        }
    }
}

