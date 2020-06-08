use futures::{try_ready, Async, AsyncSink, Poll, Future, Sink, Stream, StartSend};
use tokio;
use tokio::sync::mpsc::{channel, Receiver};

use crate::{tag, Tag, ParallelStream};

pub struct ForkRR<S: Sink> {
    pipelines: Vec<Option<S>>,
    seq_nr: usize,
}

pub fn fork_rr<S:Sink>(sinks: Vec<S>) -> ForkRR<S> {
    let mut pipelines = Vec::with_capacity(sinks.len());
    for s in sinks {
        pipelines.push(Some(s));
    }
    assert!(!pipelines.is_empty());

    ForkRR {
        pipelines,
        seq_nr: 0,
    }
}

pub fn fork_stream<S>(stream:S, degree:usize) -> ParallelStream<Receiver<Tag<S::Item>>>
where S:Stream + 'static,
S::Item: Send,
S: Send,
{
        let mut streams = Vec::new();
        let mut sinks = Vec::new();
        for _i in 0..degree {
            let (tx, rx) = channel::<Tag<S::Item>>(1);
            sinks.push(tx);
            streams.push(rx);
        }
        let fork = fork_rr(sinks);

        let fork_task = stream
            .forward(fork.sink_map_err(|e| {
                eprintln!("fork send error:{}", e);
                panic!()
        }))
        .map(|(_in, _out)| ())
        .map_err(|_e| {
            panic!()
        });

        tokio::spawn(fork_task);

        ParallelStream{streams}
}

impl<S, Item> Sink for ForkRR<S>
    where S: Sink<SinkItem=Tag<Item>>
{
    type SinkItem = Item;
    type SinkError = S::SinkError;

    fn start_send(&mut self, item: Item) -> StartSend<Item, Self::SinkError> {
        let i = self.seq_nr % self.pipelines.len();
        let sink = &mut self.pipelines[i].as_mut().expect("sink is already closed");
        let tagged_item = tag(self.seq_nr, item);
        let result = sink.start_send(tagged_item);
        match result? {
            AsyncSink::Ready => {
                self.seq_nr += 1;
                Ok(AsyncSink::Ready)
            },
            AsyncSink::NotReady(tagged) => {
                Ok(AsyncSink::NotReady(tagged.untag()))
            }
        }
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

pub struct Fork<S: Sink, FSel> {
    selector: FSel,
    pipelines: Vec<Option<S>>,
}

impl<S: Sink, FSel> Fork<S, FSel>
where
    FSel: Fn(&S::SinkItem) -> usize,
{
    pub fn new(selector: FSel, sinks: Vec<S>) -> Self {
        let mut pipelines = Vec::with_capacity(sinks.len());
        for s in sinks {
            pipelines.push(Some(s));
        }
        assert!(!pipelines.is_empty());

        Fork {
            selector,
            pipelines,
        }
    }

    /*
    pub fn add(&mut self, sink: EventSink) {
        self.pipelines.push(sink);
    }
    */
}

impl<S: Sink, FSel> Sink for Fork<S, FSel>
where
    FSel: Fn(&S::SinkItem) -> usize,
{
    type SinkItem = S::SinkItem;
    type SinkError = S::SinkError;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        let index = (self.selector)(&item) % self.pipelines.len();
        if let Some(sink) = &mut self.pipelines[index] {
            sink.start_send(item)
        } else {
            panic!("sink is already closed")
        }
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

