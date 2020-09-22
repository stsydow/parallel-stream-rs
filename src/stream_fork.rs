use futures::ready;
use futures::task::{Poll, Context};
use std::pin::Pin;
use futures::prelude::*;
use tokio;
use tokio::sync::mpsc::{channel, Receiver};
use tokio::runtime::Handle;

use crate::{ParallelStream, StreamExt};

pub struct ForkRR<S> {
    pipelines: Vec<Option<S>>,
    next_index: usize,
}

pub fn fork_rr<I, S:Sink<I>>(sinks: Vec<S>) -> ForkRR<S> {
    let mut pipelines = Vec::with_capacity(sinks.len());
    for s in sinks {
        pipelines.push(Some(s));
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
        let i = self.next_index % self.pipelines.len();
        let sink = &mut self.pipelines[i].as_mut().expect("sink is already closed");
        sink.poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: I) -> Result<(), Self::Error> {
        let i = self.next_index % self.pipelines.len();
        let sink = &mut self.pipelines[i].as_mut().expect("sink is already closed");
        sink.start_send(item)?;

        self.next_index += 1;
        Ok(())
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context) 
        -> Poll<Result<(), Self::Error>>
    {
        for iter_sink in self.pipelines.iter_mut() {
            if let Some(sink) = iter_sink {
                ready!(sink.poll_flush(cx));
            }
        }

        Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context) 
        -> Poll<Result<(), Self::Error>>
    {
        for i in 0..self.pipelines.len() {
            if let Some(sink) = &mut self.pipelines[i] {
                ready!(sink.poll_close(cx));
                self.pipelines[i] = None;
            }
        }

        Poll::Ready(Ok(()))
    }
}

pub struct ForkSel<S, FSel> {
    selector: FSel,
    pipelines: Vec<Option<S>>,
}

pub fn fork_sel<I, S:Sink<I>, Si, FSel>(sinks: Si, selector: FSel) -> ForkSel<S, FSel>
    where Si: IntoIterator<Item=S>,
{
    ForkSel {
        selector,
        pipelines: sinks.into_iter().map( |s| Some(s) ).collect(),
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
        for iter_sink in self.pipelines.iter_mut() {
            if let Some(sink) = iter_sink {
                ready!(sink.poll_ready(cx));
            }
        }

        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: I) -> Result<(), Self::Error> {
        let i = (self.selector)(&item) % self.pipelines.len();
        let sink = &mut self.pipelines[i].as_mut().expect("sink is already closed");
        sink.start_send(item)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context) 
        -> Poll<Result<(), Self::Error>>
    {
        for iter_sink in self.pipelines.iter_mut() {
            if let Some(sink) = iter_sink {
                ready!(sink.poll_flush(cx));
            }
        }

        Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context) 
        -> Poll<Result<(), Self::Error>>
    {
        for i in 0..self.pipelines.len() {
            if let Some(sink) = &mut self.pipelines[i] {
                ready!(sink.poll_close(cx));
                self.pipelines[i] = None;
            }
        }

        Poll::Ready(Ok(()))
    }
}

