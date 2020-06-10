use std::time::Instant;
use crate::LogHistogram;
use core::mem;
use futures::{Future, Poll, IntoFuture, Async};
use futures::stream::Stream;

#[derive(Debug)]
#[must_use = "streams do nothing unless polled"]
pub struct InstrumentedFold<S, F, Fut, T> where Fut: IntoFuture
{
    name: String,
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
    Processing((Instant, F)),
}

pub fn instrumented_fold<S, F, Fut, T>(s: S, f: F, t: T, name: String) -> InstrumentedFold<S, F, Fut, T>
    where S: Stream,
          F: FnMut(T, S::Item) -> Fut,
          Fut: IntoFuture<Item = T>,
          S::Error: From<Fut::Error>,
{
    InstrumentedFold {
        name,
        hist: LogHistogram::new(),
        stream: s,
        f: f,
        state: State::Ready(t),
    }
}

impl<S, F, Fut, T> Future for InstrumentedFold<S, F, Fut, T>
    where S: Stream,
          F: FnMut(T, S::Item) -> Fut,
          Fut: IntoFuture<Item = T>,
          S::Error: From<Fut::Error>,
{
    type Item = T;
    type Error = S::Error;

    fn poll(&mut self) -> Poll<T, S::Error> {
        loop {
            match mem::replace(&mut self.state, State::Empty) {
                State::Empty => panic!("cannot poll Fold twice"),
                State::Ready(state) => {
                    match self.stream.poll()? {
                        Async::Ready(Some(e)) => {
                            let start_time = Instant::now();
                            let future = (self.f)(state, e);
                            let future = future.into_future();
                            self.state = State::Processing((start_time, future));
                        }
                        Async::Ready(None) => {
                            self.hist.print_stats(&self.name);
                            return Ok(Async::Ready(state));
                        },
                        Async::NotReady => {
                            self.state = State::Ready(state);
                            return Ok(Async::NotReady)
                        }
                    }
                }
                State::Processing((start_time, mut fut)) => {
                    match fut.poll()? {
                        Async::Ready(state) => {
                            self.hist.sample_now(&start_time);
                            self.state = State::Ready(state)
                        },
                        Async::NotReady => {
                            self.state = State::Processing((start_time, fut));
                            return Ok(Async::NotReady)
                        }
                    }
                }
            }
        }
    }
}

