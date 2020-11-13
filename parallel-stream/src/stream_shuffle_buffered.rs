use futures::{try_ready, Async, Poll, Sink, StartSend};
use tokio::prelude::AsyncSink;

pub struct ShuffleBufferedInput<S: Sink, FSel, Item> {
    selector: FSel,
    pipelines: Vec<Option<S>>,
    buffers: Vec<Vec<Item>>,
}

pub struct ShuffleBuffered<S: Sink + Clone, FSel> {
    selector: FSel,
    pipelines: Vec<Option<S>>,
}

impl<S, Item, FSel> ShuffleBuffered<S, FSel>
where
    S:Sink<SinkItem=Vec<Item>> + Clone,
    FSel: Fn(&Item) -> usize + Copy,
{
    pub fn new(selector: FSel, sinks: Vec<S>) -> Self {
        let mut pipelines = Vec::with_capacity(sinks.len());
        for s in sinks {
            pipelines.push(Some(s));
        }
        assert!(!pipelines.is_empty());

        ShuffleBuffered {
            selector,
            pipelines,
        }
    }

    pub fn create_input(&mut self) -> ShuffleBufferedInput<S, FSel, Item> {
        let pipe_count = self.pipelines.len();

        let mut buffers = Vec::with_capacity(pipe_count);
        for _i in 0 .. pipe_count {
            buffers.push(Vec::new())
        }

        ShuffleBufferedInput {
            selector: self.selector,
            pipelines: self.pipelines.clone(), // TODO JoinOrder here?
            buffers,
        }
    }

    /*
    pub fn add(&mut self, sink: EventSink) {
        self.pipelines.push(sink);
    }
    */
}

impl<S, FSel, Item> ShuffleBufferedInput<S, FSel, Item>
where
    S: Sink<SinkItem=Vec<Item>> + Clone,
    FSel: Fn(&Item) -> usize + Copy
{
    pub fn is_clear(&self) -> bool {
        self.buffers.iter().find(|buf| buf.len() > 0).is_none()
    }

    pub fn try_send_all(&mut self) -> Result<bool,S::SinkError> {
        let mut all_empty = true;
        let pipeline_count = self.pipelines.len();

        for i in 0..pipeline_count {
            if self.buffers[i].is_empty() { continue; }
            self.buffers.push(Vec::new());
            let buf = self.buffers.swap_remove(i);


            let sink = self.pipelines[i].as_mut().expect("sink is already closed");
            match sink.start_send(buf)? {
                AsyncSink::Ready => {},
                AsyncSink::NotReady(buf) => {
                    self.buffers[i] = buf;
                    all_empty = false;
                }
            }
        }

        Ok(all_empty)
    }
}

impl<S, FSel, Item> Sink for ShuffleBufferedInput<S, FSel, Item>
where
    S: Sink<SinkItem=Vec<Item>> + Clone,
    FSel: Fn(&Item) -> usize + Copy,
{
    type SinkItem = Vec<Item>;
    type SinkError = S::SinkError;

    fn start_send(&mut self, chunk: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        if !self.try_send_all()? {
            return Ok(AsyncSink::NotReady(chunk));
        }

        let pipeline_count = self.pipelines.len();
        let spare_capacity = 2 * chunk.len()/pipeline_count;
        for buf in &mut self.buffers {
                buf.reserve(spare_capacity);
        }

        let selector = &self.selector;
        for item in chunk {
            let index = (selector)(&item) % pipeline_count;
            self.buffers[index].push(item);
        }
        self.try_send_all() ?;

        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {

        let state = if self.try_send_all()? {
            for iter_sink in self.pipelines.iter_mut() {
                if let Some(sink) = iter_sink {
                    try_ready!(sink.poll_complete());
                }
            }
            Async::Ready(())
        } else {
            Async::NotReady
        };

        Ok(state)
    }

    fn close(&mut self) -> Poll<(), Self::SinkError> {

        let state = if self.try_send_all()? {
            for i in 0..self.pipelines.len() {
                if let Some(sink) = &mut self.pipelines[i] {
                    try_ready!(sink.close());
                    self.pipelines[i] = None;
                }
            }
            Async::Ready(())
        } else {
            Async::NotReady
        };

        Ok(state)
    }
}

