use futures::ready;
use futures::task::{Poll, Context};
use std::pin::Pin;
use futures::prelude::*;
use tokio;
use tokio::sync::mpsc::{channel, Receiver};
use tokio::runtime::Handle;

use crate::{ParallelStream, StreamExt};
use std::iter::FromIterator;

struct Pipe<S, I>
where S: Sink<I>,
{
    sink: Option<S>,
    buffer: Option<I>,
}

pub struct ChunkedFork<I, S: Sink<I>, FSel>
{
    selector: FSel,
    pipelines: Vec<Pipe<S, I>>,
}


pub fn chunked_fork<S, Si, FSel, Item>(sinks: Si, selector: FSel) -> ChunkedFork<Vec<Item>, S, FSel>
where
    S: Sink<Vec<Item>>,
    Si: IntoIterator<Item=S>,
    FSel: Fn(&Item) -> usize,
{
    ChunkedFork {
        selector,
        pipelines: sinks.into_iter().map( |s| Pipe{sink: Some(s), buffer:  None} ).collect(),
    }
}


pub fn fork_stream_sel_chunked<S, FSel, C>(stream:S, selector: FSel, degree:usize, buf_size: usize, exec: &mut Handle) -> ParallelStream<Receiver<Vec<C::Item>>>
where
    C: IntoIterator,
    S: Stream<Item=C> + Send + 'static,
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

impl<S, Item, FSel> ChunkedFork<Vec<Item>, S, FSel>
where
    S: Sink<Vec<Item>>,
{
    pub fn is_clear(&self) -> bool {
        ! self.pipelines.iter().all(|pipe| {
            pipe.buffer.is_none()
        })
    }

    pub fn try_send_all(&mut self, cx: &mut Context) -> Result<bool,S::Error> {
        let mut all_empty = true;

        for pipe in self.pipelines.iter_mut() {
            let sink = pipe.sink.as_mut().unwrap();
            match sink.poll_ready(cx) {
                Poll::Ready(Ok()) => {
                    let buffer = pipe.buffer.take();
                    if let Some(buf) = buffer {
                        sink.start_send(buf)?

                    }
                    ()
                },
                Poll::Ready(Err(e)) => {
                    return Err(e)        
                },
                Poll::Pending => {
                        all_empty = false;
                }
            }
        }

        Ok(all_empty)
    }
}

impl<I, S, FSel> Sink<Vec<I>> for ChunkedFork<Vec<I>, S, FSel>
where
    //C: IntoIterator<Item=I>,
    FSel: Fn(&I) -> usize,
    S: Sink<Vec<I>>
{
    type Error = S::Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut Context
        ) -> Poll<Result<(), Self::Error>> 
    {
        if !self.try_send_all()? {
            return Poll::Pending;
        }
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, chunk: Vec<I>) -> Result<(), Self::Error> {

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

        Ok(())
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context
    ) -> Poll<Result<(), Self::Error>> {

        let state = if self.try_send_all()? {
            for iter_sink in self.pipelines.iter_mut() {
                if let Some(ref mut sink) = iter_sink.sink {
                    ready!(sink.poll_flush(cx));
                }
            }
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        };

        state
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context
    ) -> Poll<Result<(), Self::Error>> {

        let state = if self.try_send_all()? {
            for iter_sink in self.pipelines.iter_mut() {
                if let Some(ref mut sink) = iter_sink.sink {
                    if iter_sink.buffer.is_none(){
                        ready!(sink.poll_close(cx));
                        iter_sink.sink = None;
                    } else {
                         return Poll::Pending;
                    }
                }
            }
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        };

        state
    }
}
