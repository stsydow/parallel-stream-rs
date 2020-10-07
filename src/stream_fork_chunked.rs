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
use std::iter::FromIterator;

struct Pipe<S, I>
where S: Sink<I>,
{
    sink: Option<Pin<Box<S>>>,
    buffer: Option<I>,
}

#[pin_project(project = ChunkedForkProj)]
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
        pipelines: sinks.into_iter().map( |s| Pipe{sink: Some(Box::pin(s)), buffer:  None} ).collect(),
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

    pub fn try_send_all(self: Pin<&mut Self>, cx: &mut Context) -> Result<bool,S::Error> {
        let mut all_empty = true;

        let this = self.project();
        for pipe in this.pipelines.iter_mut() {
            let sink = pipe.sink.as_mut().unwrap().as_mut();
            match sink.poll_ready(cx) {
                Poll::Ready(Ok(())) => {
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
        if !self.try_send_all(cx)? {
            return Poll::Pending;
        }
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, chunk: Vec<I>) -> Result<(), Self::Error> {

        let this = self.project();
        let degree = this.pipelines.len();

        for item in chunk {
            let i = (this.selector)(&item) % degree;
            if let Some(ref mut buf) = this.pipelines[i].buffer {
                buf.push(item);
            }else {
                this.pipelines[i].buffer = Some(vec![item]);
            }
        }

        Ok(())
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context
    ) -> Poll<Result<(), Self::Error>> {

        let state = if self.try_send_all(cx)? {
            let this = self.project();
            for iter_sink in this.pipelines.iter_mut() {
                if let Some(ref mut sink) = iter_sink.sink {
                    ready!(sink.as_mut().poll_flush(cx));
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

        let state = if self.try_send_all(cx)? {
            let this = self.project();
            for iter_sink in this.pipelines.iter_mut() {
                if let Some(ref mut sink) = iter_sink.sink {
                    if iter_sink.buffer.is_none(){
                        ready!(sink.as_mut().poll_close(cx));
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
