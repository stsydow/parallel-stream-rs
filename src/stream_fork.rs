use futures::{try_ready, Async, Poll, Future, Sink, Stream, StartSend};
use tokio;
use tokio::sync::mpsc::{channel, Receiver};
use tokio::executor::Executor;

use crate::{ParallelStream, StreamExt};

pub struct ForkRR<S> {
    pipelines: Vec<Option<S>>,
    next_index: usize,
}

pub fn fork_rr<S:Sink>(sinks: Vec<S>) -> ForkRR<S> {
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

pub fn fork_stream<S, E: Executor>(stream:S, degree:usize, exec: &mut E) -> ParallelStream<Receiver<S::Item>>
where S:Stream + 'static,
S::Item: Send,
S::Error: std::fmt::Debug,
S: Send,
{
        let mut streams = Vec::new();
        let mut sinks = Vec::new();
        for _i in 0..degree {
            let (tx, rx) = channel::<S::Item>(2);
            sinks.push(tx);
            streams.push(rx);
        }
        let fork = fork_rr(sinks);

        stream.forward_and_spawn(fork, exec);

        ParallelStream::from(streams)
}

impl<S, Item> Sink for ForkRR<S>
    where S: Sink<SinkItem=Item>
{
    type SinkItem = Item;
    type SinkError = S::SinkError;

    fn start_send(&mut self, item: Item) -> StartSend<Item, Self::SinkError> {
        let i = self.next_index % self.pipelines.len();
        let sink = &mut self.pipelines[i].as_mut().expect("sink is already closed");
        let result = sink.start_send(item)?;
        if result.is_ready() {
            self.next_index += 1;
        }
        Ok(result)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        for iter_sink in self.pipelines.iter_mut() {
            if let Some(sink) = iter_sink {
                try_ready!(sink.poll_complete());
            }
        }

        Ok(Async::Ready(()))
    }

    fn close(&mut self) -> Poll<(), Self::SinkError> {
        for i in 0..self.pipelines.len() {
            if let Some(sink) = &mut self.pipelines[i] {
                try_ready!(sink.close());
                self.pipelines[i] = None;
            }
        }

        Ok(Async::Ready(()))
    }
}

pub struct ForkSel<S, FSel> {
    selector: FSel,
    pipelines: Vec<Option<S>>,
}

pub fn fork_sel<S:Sink, Si, FSel>(sinks: Si, selector: FSel) -> ForkSel<S, FSel>
    where Si: IntoIterator<Item=S>,
{
    ForkSel {
        selector,
        pipelines: sinks.into_iter().map( |s| Some(s) ).collect(),
    }
}

//TODO tag -> fork_stream_sel -> tag
//TODO raw -> fork_stream_sel -> raw
pub fn fork_stream_sel<S, FSel, E: Executor>(stream:S, selector: FSel, degree:usize, exec: &mut E) -> ParallelStream<Receiver<S::Item>>
where S:Stream + 'static,
S::Item: Send,
S::Error: std::fmt::Debug,
S: Send,
FSel: Fn(&S::Item) -> usize + Send + 'static,
{
        let mut streams = Vec::new();
        let mut sinks = Vec::new();
        for _i in 0..degree {
            let (tx, rx) = channel::<S::Item>(2);
            sinks.push(tx);
            streams.push(rx);
        }
        let fork = fork_sel(sinks, selector);

        stream.forward_and_spawn(fork, exec);

        ParallelStream::from(streams)
}


impl<S, FSel> Sink for ForkSel<S, FSel>

where S: Sink,
      S::SinkItem: Send,
      FSel: Fn(&S::SinkItem) -> usize,
{
    type SinkItem = S::SinkItem;
    type SinkError = S::SinkError;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        let i = (self.selector)(&item) % self.pipelines.len();
        let sink = &mut self.pipelines[i].as_mut().expect("sink is already closed");
        let result = sink.start_send(item)?;
        Ok(result)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        for iter_sink in self.pipelines.iter_mut() {
            if let Some(sink) = iter_sink {
                try_ready!(sink.poll_complete());
            }
        }

        Ok(Async::Ready(()))
    }

    fn close(&mut self) -> Poll<(), Self::SinkError> {
        for i in 0..self.pipelines.len() {
            if let Some(sink) = &mut self.pipelines[i] {
                try_ready!(sink.close());
                self.pipelines[i] = None;
            }
        }

        Ok(Async::Ready(()))
    }
}

