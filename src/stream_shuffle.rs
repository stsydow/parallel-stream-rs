use futures::{try_ready, Async, Poll, Sink, StartSend};

pub struct ShuffleInput<S: Sink, FSel> {
    selector: FSel,
    pipelines: Vec<Option<S>>,
}

pub struct Shuffle<S: Sink + Clone, FSel> {
    selector: FSel,
    pipelines: Vec<Option<S>>,
}

impl<S: Sink + Clone, FSel> Shuffle<S, FSel>
where
    FSel: Fn(&S::SinkItem) -> usize + Copy,
{
    pub fn new(selector: FSel, sinks: Vec<S>) -> Self {
        let mut pipelines = Vec::with_capacity(sinks.len());
        for s in sinks {
            pipelines.push(Some(s));
        }
        assert!(!pipelines.is_empty());

        Shuffle {
            selector,
            pipelines
        }
    }

    pub fn create_input(&mut self) -> ShuffleInput<S, FSel> {
        ShuffleInput {
            selector: self.selector,
            pipelines: self.pipelines.clone()
        }
    }

    /*
    pub fn add(&mut self, sink: EventSink) {
        self.pipelines.push(sink);
    }
    */
}

impl<S: Sink + Clone, FSel> Sink for ShuffleInput<S, FSel>
where
    FSel: Fn(&S::SinkItem) -> usize + Copy,
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

