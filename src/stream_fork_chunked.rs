use futures::{try_ready, Stream, Async, AsyncSink, Poll, Sink, StartSend};

use tokio::sync::mpsc::{channel, Receiver};
use tokio::executor::Executor;
use crate::{ParallelStream, StreamExt};
use std::iter::FromIterator;

struct Pipe<S>
where S: Sink,
{
    sink: Option<S>,
    buffer: Option<S::SinkItem>,
}

pub struct ChunkedFork<S: Sink, FSel>
where S: Sink,
{
    selector: FSel,
    pipelines: Vec<Pipe<S>>,
}


pub fn chunked_fork<S, Si, FSel, Item>(sinks: Si, selector: FSel) -> ChunkedFork<S, FSel>
where
    S: Sink<SinkItem=Vec<Item>>,
    Si: IntoIterator<Item=S>,
    FSel: Fn(&Item) -> usize,
{
    ChunkedFork {
        selector,
        pipelines: sinks.into_iter().map( |s| Pipe{sink: Some(s), buffer:  None} ).collect(),
    }
}


pub fn fork_stream_sel_chunked<S, FSel, C, E:Executor>(stream:S, selector: FSel, degree:usize, buf_size: usize, exec: &mut E) -> ParallelStream<Receiver<Vec<C::Item>>>
where
    C: IntoIterator,
    S: Stream<Item=C> + Send + 'static,
    S::Error: std::fmt::Debug,
    C::Item: Send + 'static,
    FSel: Fn(&C::Item) -> usize + Send + 'static,
{
        let mut streams = Vec::new();
        let mut sinks = Vec::new();
        for _i in 0..degree {
            let (tx, rx) = channel::<Vec<C::Item>>(buf_size);
            sinks.push(tx);
            streams.push(rx);
        }
        let fork = chunked_fork(sinks, selector);

        stream.map(|c| Vec::from_iter(c.into_iter())).forward_and_spawn(fork, exec);
        ParallelStream::from(streams)
}

impl<S, Item, FSel> ChunkedFork<S, FSel>
where
    S: Sink<SinkItem=Vec<Item>>,
{
    pub fn is_clear(&self) -> bool {
        ! self.pipelines.iter().all(|pipe| {
            pipe.buffer.is_none()
        })
    }

    pub fn try_send_all(&mut self) -> Result<bool,S::SinkError> {
        let mut all_empty = true;

        for pipe in self.pipelines.iter_mut() {
            let buffer =  pipe.buffer.take();
            if let Some(buf) = buffer {
                let sink = pipe.sink.as_mut().unwrap();
                match sink.start_send(buf)? {
                    AsyncSink::Ready => {
                        () //pipe.buffer = None;
                    },
                    AsyncSink::NotReady(buf) => {
                        pipe.buffer = Some(buf);
                        all_empty = false;
                    }
                }

            }
        }

        Ok(all_empty)
    }
}

impl<I, S, FSel> Sink for ChunkedFork<S, FSel>
where
    //C: IntoIterator<Item=I>,
    FSel: Fn(&I) -> usize,
    S: Sink<SinkItem=Vec<I>>
{
    type SinkItem = Vec<I>;
    type SinkError = S::SinkError;

    fn start_send(&mut self, chunk: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        if !self.try_send_all()? {
            return Ok(AsyncSink::NotReady(chunk));
        }

        let degree = self.pipelines.len();

        for item in chunk {
            let i = (self.selector)(&item) % degree;
            if let Some(ref mut buf) = self.pipelines[i].buffer {
                buf.push(item);
            }else {
                self.pipelines[i].buffer = Some(vec![item]);
            }
        }
        self.try_send_all()?;

        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {

        let state = if self.try_send_all()? {
            for iter_sink in self.pipelines.iter_mut() {
                if let Some(ref mut sink) = iter_sink.sink {
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
            for iter_sink in self.pipelines.iter_mut() {
                if let Some(ref mut sink) = iter_sink.sink {
                    if iter_sink.buffer.is_none(){
                        try_ready!(sink.close());
                        iter_sink.sink = None;
                    } else {
                         return Ok(Async::NotReady);
                    }
                }
            }
            Async::Ready(())
        } else {
            Async::NotReady
        };

        Ok(state)
    }
}
