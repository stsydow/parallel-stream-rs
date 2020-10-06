#[cfg(stream_profiling)]
use std::time::Instant;
#[cfg(stream_profiling)]
use crate::LogHistogram;
//use core::mem;
use futures::{ready, Future, Stream};
use futures::task::{Poll, Context};
use std::pin::Pin;
use pin_project::pin_project;
//use futures::prelude::*;
//use std::future::Future;

#[pin_project(project = InstrumentedFoldProj)]
#[derive(Debug)]
#[must_use = "streams do nothing unless polled"]
pub struct InstrumentedFold<S, F, Fut, T> where Fut: Future
{
    #[cfg(stream_profiling)]
    name: String,
    #[cfg(stream_profiling)]
    hist: LogHistogram,
    #[pin]
    stream: S,
    f: F,
    accum: Option<T>,
    #[pin]
    future: Option<Fut>,
}

pub fn instrumented_fold<S, F, Fut, T>(s: S, f: F, t: T, name: String) -> InstrumentedFold<S, F, Fut, T>
    where S: Stream,
          F: FnMut(T, S::Item) -> Fut,
          Fut: Future<Output = T>,
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
        accum:Some(t),
        future: None,
    }
}

impl<S, F, Fut, T> Future for InstrumentedFold<S, F, Fut, T>
    where S: Stream,
          F: FnMut(T, S::Item) -> Fut,
          Fut: Future<Output = T>,
{
    type Output = T;

    #[cfg(stream_profiling)]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T>
    {
        let InstrumentedFoldProj { name, hist, mut stream, f, accum, mut future } = self.project();
        Poll::Ready(loop {
            if let Some(fut) = future.as_mut().as_pin_mut() {
                // we're currently processing a future to produce a new accum value
                unimplemented!();//TODO let start_time = Instant::now();
                accum = Some(ready!(fut.poll(cx)));
                future.set(None);
                unimplemented!();//TODO hist.sample_now(&start_time);
            } else if accum.is_some() {
                // we're waiting on a new item from the stream
                let res = ready!(stream.as_mut().poll_next(cx));
                let a = accum.take().unwrap();
                if let Some(item) = res {
                    future.set(Some(f(a, item)));
                } else {
                    hist.print_stats(&name);
                    break a;
                }
            } else {
                panic!("Fold polled after completion")
            }
        })
/*
        loop {
            match mem::replace(&mut state, State::Empty) {
                State::Empty => panic!("cannot poll Fold twice"),
                State::Ready(state) => {
                    match stream.poll_next(cx) {
                        Poll::Ready(Some(e)) => {
                            let start_time = Instant::now();
                            let future = (f)(state, e);
                            let future = future.into_future();
                            state = State::Processing((start_time, future)); //TODO: we messure scheduling here!
                        }
                        Poll::Ready(None) => {
                            hist.print_stats(&name);
                            return Ok(Poll::Ready(state));
                        },
                        Poll::Pending => {
                            state = State::Ready(state);
                            return Ok(Poll::Pending)
                        }
                    }
                }
                State::Processing((start_time, mut fut)) => {
                    match fut.poll(cx) {
                        Poll::Ready(state) => {
                            hist.sample_now(&start_time);
                            state = State::Ready(state)
                        },
                        Poll::Pending => {
                            state = State::Processing((start_time, fut));
                            return Ok(Poll::Pending)
                        }
                    }
                }
            }
        }
        */
    }

    #[cfg(not(stream_profiling))]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T>
    {
        let InstrumentedFoldProj {mut stream, f, accum, mut future } = self.project();
        Poll::Ready(loop {
            if let Some(fut) = future.as_mut().as_pin_mut() {
                // we're currently processing a future to produce a new accum value
                *accum = Some(ready!(fut.poll(cx)));
                future.set(None);
            } else if accum.is_some() {
                // we're waiting on a new item from the stream
                let res = ready!(stream.as_mut().poll_next(cx));
                let a = accum.take().unwrap();
                if let Some(item) = res {
                    future.set(Some(f(a, item)));
                } else {
                    break a;
                }
            } else {
                panic!("Fold polled after completion")
            }
        })
    }
}

