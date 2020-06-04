use futures::{try_ready, Async, Poll, Sink, StartSend};

pub struct ChunkedFork<S: Sink, FSel, Item> {
    selector: FSel,
    pipelines: Vec<Option<S>>,
    buffers: Vec<Vec<Item>>,
    capacity: usize,
}

impl<S: Sink<SinkItem=Vec<Item>>, FSel, Item> ChunkedFork<S, FSel, Item>
    where
        FSel: Fn(&Item) -> usize,
{
    pub fn new(selector: FSel, sinks: Vec<S>, capacity:usize) -> Self {
        let mut pipelines = Vec::with_capacity(sinks.len());
        let mut buffers = Vec::with_capacity(sinks.len());
        for s in sinks {
            pipelines.push(Some(s));
            buffers.push(Vec::with_capacity(capacity));
        }
        assert!(!pipelines.is_empty());

        ChunkedFork {
            selector,
            pipelines,
            buffers,
            capacity,
        }
    }

    /*
    pub fn add(&mut self, sink: EventSink) {
        self.pipelines.push(sink);
    }
    */
}


impl<S: Sink<SinkItem=Vec<Item>>, FSel> Sink for ChunkedFork<S, FSel, Item>
where
    FSel: Fn(&Item) -> usize,
{
    type SinkItem = S::SinkItem;
    type SinkError = S::SinkError;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        let index = (self.selector)(&item) % self.pipelines.len();
        if let Some(sink) = &mut self.pipelines[index] {
            let buffer = &mut self.buffers[index];
            if buffer.len() >= self.capacity {
                self.buffers.push(Vec::with_capacity(self.capacity));
                let buf = self.buffers.swap_remove(index);
                TODO
                sink.start_send(buf)
            } else {
                buffer.push(item)
            }
        } else {
            panic!("sink is already closed")
        }
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        for i in 0..self.pipelines.len() {
            if let Some(sink) = &mut self.pipelines[i] {
                if !self.buffers[i].empty() {
                    self.buffers.push(Vec::new())
                    let buf = self.buffers.swap_remove(i);
                    TODO
                    sink.start_send(buf)
                }
                try_ready!(sink.poll_complete());
            }
        }

        Ok(Async::Ready(()))
    }

    fn close(&mut self) -> Poll<(), Self::SinkError> {
        for i in 0..self.pipelines.len() {
            if let Some(sink) = &mut self.pipelines[i] {
                TODO
                try_ready!(sink.close());
                self.pipelines[i] = None;
            }
        }

        Ok(Async::Ready(()))
    }
}
