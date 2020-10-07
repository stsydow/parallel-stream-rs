use futures::ready;
use futures::task::{Poll, Context};
use std::pin::Pin;
use pin_project::pin_project;
use futures::prelude::*;
use tokio;
//use tokio::sync::mpsc::{channel, Receiver};
use futures::channel::mpsc::{channel, Receiver};
use tokio::runtime::Handle;

use crate::{ParallelStream, StreamExt};

#[pin_project(project = ForkRRProj)]
pub struct ForkRR<S> {
    pipelines: Vec<Option<Pin<Box<S>>>>,
    next_index: usize,
}

pub fn fork_rr<I, S:Sink<I>>(sinks: Vec<S>) -> ForkRR<S> {
    let mut pipelines = Vec::with_capacity(sinks.len());
    for s in sinks {
        pipelines.push(Some(Box::pin(s)));
    }
    assert!(!pipelines.is_empty());

    ForkRR {
        pipelines,
        next_index: 0,
    }
}

pub fn fork_stream<S>(stream:S, degree:usize, buf_size: usize, exec: &mut Handle) -> ParallelStream<Receiver<S::Item>>
where S:Stream + 'static,
S::Item: Send,
S: Send,
{
        let mut streams = Vec::new();
        let mut sinks = Vec::new();
        for _i in 0..degree {
            let (tx, rx) = channel::<S::Item>(buf_size);
            sinks.push(tx);
            streams.push(rx);
        }
        let fork = fork_rr(sinks);

        stream.forward_and_spawn(fork, exec);

        ParallelStream::from(streams)
}

impl<S, I> Sink<I> for ForkRR<S>
    where S: Sink<I>
{
    type Error = S::Error;

    fn poll_ready(
        self: Pin<&mut Self>, 
        cx: &mut Context) 
    -> Poll<Result<(), Self::Error>> 
    {
        let this = self.project();
        let i = *this.next_index % this.pipelines.len();
        let maybe_sink = &mut this.pipelines[i]; 
        let sink: Pin<&mut S> = maybe_sink.as_mut().expect("sink is already closed").as_mut();
        sink.poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: I) -> Result<(), Self::Error> {
        let this = self.project();
        let i = *this.next_index % this.pipelines.len();
        let sink = this.pipelines[i].as_mut().expect("sink is already closed").as_mut();
        sink.start_send(item)?;

        *this.next_index += 1;
        Ok(())
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context) 
        -> Poll<Result<(), Self::Error>>
    {
        let this = self.project();
        for iter_sink in this.pipelines.iter_mut() {
            if let Some(sink) = iter_sink {
                ready!(sink.as_mut().poll_flush(cx));
            }
        }

        Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context) 
        -> Poll<Result<(), Self::Error>>
    {
        let this = self.project();
        for i in 0..this.pipelines.len() {
            if let Some(ref mut sink) = this.pipelines[i] {
                ready!(sink.as_mut().poll_close(cx));
                this.pipelines[i] = None;
            }
        }

        Poll::Ready(Ok(()))
    }
}

#[pin_project(project = ForkSelProj)]
pub struct ForkSel<S, FSel> {
    selector: FSel,
    pipelines: Vec<Option<Pin<Box<S>>>>,
}

pub fn fork_sel<I, S:Sink<I>, Si, FSel>(sinks: Si, selector: FSel) -> ForkSel<S, FSel>
    where Si: IntoIterator<Item=S>,
{
    ForkSel {
        selector,
        pipelines: sinks.into_iter().map( |s| Some(Box::pin(s)) ).collect(),
    }
}

pub fn fork_stream_sel<S, FSel>(stream:S, selector: FSel, degree:usize, buf_size: usize, exec: &mut Handle) -> ParallelStream<Receiver<S::Item>>
where S:Stream + 'static,
S::Item: Send,
S: Send,
FSel: Fn(&S::Item) -> usize + Send + 'static,
{
        let mut streams = Vec::new();
        let mut sinks = Vec::new();
        for _i in 0..degree {
            let (tx, rx) = channel::<S::Item>(buf_size);
            sinks.push(tx);
            streams.push(rx);
        }
        let fork = fork_sel(sinks, selector);

        stream.forward_and_spawn(fork, exec);

        ParallelStream::from(streams)
}


impl<S, I, FSel> Sink<I> for ForkSel<S, FSel>

where S: Sink<I>,
      I: Send,
      FSel: Fn(&I) -> usize,
{
    type Error = S::Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut Context) 
        -> Poll<Result<(), Self::Error>>
    {
        let this = self.project();
        for iter_sink in this.pipelines.iter_mut() {
            if let Some(sink) = iter_sink {
                ready!(sink.as_mut().poll_ready(cx));
            }
        }

        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: I) -> Result<(), Self::Error> {
        let this = self.project();
        let i = (this.selector)(&item) % this.pipelines.len();
        let sink = this.pipelines[i].as_mut().expect("sink is already closed").as_mut();
        sink.start_send(item)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context) 
        -> Poll<Result<(), Self::Error>>
    {
        let this = self.project();
        for iter_sink in this.pipelines.iter_mut() {
            if let Some(sink) = iter_sink {
                ready!(sink.as_mut().poll_flush(cx));
            }
        }

        Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context) 
        -> Poll<Result<(), Self::Error>>
    {
        let this = self.project();
        for i in 0..this.pipelines.len() {
            if let Some(ref mut sink) = this.pipelines[i] {
                ready!(sink.as_mut().poll_close(cx));
                this.pipelines[i] = None;
            }
        }

        Poll::Ready(Ok(()))
    }
}

