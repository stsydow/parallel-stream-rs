use futures::{try_ready, Async, Poll, Future, Sink, Stream, StartSend};
use tokio;
use tokio::sync::mpsc::{channel, Receiver};

pub struct ForkRR<S: Sink> {
    pipelines: Vec<Option<S>>,
    cursor: usize,
}

impl<S: Sink> ForkRR<S> {
    pub fn new(sinks: Vec<S>) -> Self {
        let mut pipelines = Vec::with_capacity(sinks.len());
        for s in sinks {
            pipelines.push(Some(s));
        }
        assert!(!pipelines.is_empty());

        ForkRR {
            pipelines,
            cursor: 0,
        }
    }
}

pub fn fork_stream<S>(stream:S, degree:usize) -> Vec<Receiver<S::Item>>
where S:Stream + 'static,
S::Error: std::fmt::Display,
S::Item: Send,
S: Send,
{
        let mut streams = Vec::new();
        let mut sinks = Vec::new();
        for _i in 0..degree {
            let (tx, rx) = channel::<S::Item>(1);
            sinks.push(tx);
            streams.push(rx);
        }
        let fork = ForkRR::new(sinks);

        let fork_task = stream
            .forward(fork.sink_map_err(|e| {
                eprintln!("fork send error:{}", e);
                panic!()
        }))
        .map(|(_in, _out)| ())
        .map_err(|e| {
            eprintln!("error: {}", e);
            panic!()
        });

        tokio::spawn(fork_task);

        streams
}

impl<S: Sink> Sink for ForkRR<S> {
    type SinkItem = S::SinkItem;
    type SinkError = S::SinkError;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        let i = self.cursor;
        let next_cursor = (i + 1) % self.pipelines.len();
        let sink = &mut self.pipelines[i].as_mut().expect("sink is already closed");
        self.cursor = next_cursor;
        sink.start_send(item)
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

