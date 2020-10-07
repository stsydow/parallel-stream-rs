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
    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>
        ) -> Poll<Result<(), S::Error>> 
    {
        for pipe in self.pipelines.iter_mut() {
            let sink = pipe.sink.as_mut().unwrap().as_mut();
            match sink.poll_ready(cx) {
                Poll::Ready(Ok(())) => {
                    //continue;
                    ()
                },
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Err(e))        
                },
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }

        Poll::Ready(Ok(()))
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
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>
        ) -> Poll<Result<(), Self::Error>> 
    {
        (*self).poll_ready(cx)
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

        for pipe in this.pipelines.iter_mut() {
            let sink = pipe.sink.as_mut().unwrap().as_mut();
            let buffer = pipe.buffer.take();
            if let Some(buf) = buffer {
                sink.start_send(buf)?
            }
        }

        Ok(())
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context
    ) -> Poll<Result<(), Self::Error>> {

        let this = self.project();
        for iter_sink in this.pipelines.iter_mut() {
            if let Some(ref mut sink) = iter_sink.sink {
                let r = ready!(sink.as_mut().poll_flush(cx));
                if let Err(_) = r {
                    return Poll::Ready(r);
                }
            }
        }
        Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context
    ) -> Poll<Result<(), Self::Error>> {

        let this = self.project();
        for iter_sink in this.pipelines.iter_mut() {
            if let Some(ref mut sink) = iter_sink.sink {
                if iter_sink.buffer.is_none(){
                    let r = ready!(sink.as_mut().poll_close(cx));
                    if let Err(_) = r {
                        return Poll::Ready(r);
                    }
                    iter_sink.sink = None;
                } else {
                    return Poll::Pending;
                }
            }
        }
        Poll::Ready(Ok(()))
    }
}
