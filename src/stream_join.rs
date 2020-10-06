use futures::ready;
use futures::task::{Poll, Context};
use std::pin::Pin;
use pin_project::pin_project;
use futures::prelude::*;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::{Tag, ParallelStream};

/*
struct TaggedQueueItem<I> {
    event: Option<Tag<I>>,
    source_id: usize,
}

impl<I> Ord for TaggedQueueItem<I> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.event.cmp(&other.event).reverse()
    }
}

impl<I> PartialOrd for TaggedQueueItem<I> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<I> PartialEq for TaggedQueueItem<I> {
    fn eq(&self, other: &Self) -> bool {
        self.event == other.event
    }
}

impl<I> Eq for TaggedQueueItem<I> {}

*/

struct Input<S:Stream> {
    stream: Option<Pin<Box<S>>>,
    buffer: Vec<S::Item> // Tag<_>
}

impl<S:Stream> Input<S> {
    fn is_closed(&self) -> bool {
        self.stream.is_none() && self.buffer.is_empty()
    }
}

#[pin_project(project = JoinTaggedProj)]
pub struct JoinTagged<S:Stream>
{
    #[pin]
    pipelines: Vec<Input<S>>,
    next_tag: usize,
}

pub fn join_tagged<S:Stream>(par_stream: ParallelStream<S>) -> JoinTagged<S>
{
    /*
    let mut queue = BinaryHeap::new();
    for i in 0 .. par_stream.streams.len() {
        queue.push(TaggedQueueItem{event: None, source_id: i});
    }
    */
    let degree = par_stream.width();
    let mut pipelines = Vec::with_capacity(degree);
    let streams: Vec<S> = par_stream.into();
    for stream in streams {
        pipelines.push( Input{ stream: Some(Box::pin(stream)), buffer: Vec::new()});
    }

    JoinTagged {
        pipelines,
        next_tag: 0,
    }
}


impl<S, I> Stream for JoinTagged<S>
where
    S: Stream<Item=Tag<I>>,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>>
    {

        let JoinTaggedProj{pipelines, next_tag} = self.project();
        pipelines.retain(|p| ! p.is_closed());

        if pipelines.is_empty() {
            return Poll::Ready(None);
        }

        for i in 0 .. pipelines.len() {
            let pipe: &mut Input<S> = &mut pipelines[i];

            if pipe.buffer.len() > 0 {
                let item = pipe.buffer.first().unwrap();
                if item.nr() == *next_tag {
                    let item = pipe.buffer.pop().unwrap();
                    *next_tag += 1;
                    return Poll::Ready(Some(item));
                }
                //continue;
            } else {
                if let Some(ref mut stream) = &mut pipe.stream {
                    match Pin::new(stream).poll_next(cx) {
                        Poll::Ready(Some(item)) => {
                            assert!(item.nr() >= *next_tag);
                            if item.nr() == *next_tag {
                                *next_tag += 1;
                                return Poll::Ready(Some(item));
                            } else {
                                pipe.buffer.push(item);
                                //continue;
                            }
                        },
                        Poll::Ready(None) => {
                            pipe.stream = None;
                            return self.poll_next(cx);
                        },
                        Poll::Pending => {
                            //continue;
                        }
                    };
                };
            };
        };
        /*
        eprintln!("ret not ready size{}", self.pipelines.len());
        for pipe in &mut self.pipelines {
            eprintln!("closed {} buf {}", pipe.stream.is_none(), pipe.buffer.len());
        }
        */
        Poll::Pending
    }
}

struct QueueItem<Event> {
    event: Option<Event>,
    order: u64,
    pipeline_index: usize,
}

impl<Event> Ord for QueueItem<Event> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.order.cmp(&other.order).reverse()
    }
}

impl<Event> PartialOrd for QueueItem<Event> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<Event> PartialEq for QueueItem<Event> {
    fn eq(&self, other: &Self) -> bool {
        self.order == other.order
    }
}

impl<Event> Eq for QueueItem<Event> {}

#[pin_project(project = JoinProj)]
pub struct Join<EventStream, FOrd>
where
    EventStream: Stream, //where FOrd: Fn(&Event) -> u64
{
    calc_order: FOrd,
    #[pin]
    pipelines: Vec<Pin<Box<EventStream>>>,
    last_values: BinaryHeap<QueueItem<EventStream::Item>>,
}

impl<EventStream, FOrd> Join<EventStream, FOrd>
where
    EventStream: Stream,
    FOrd: Fn(&EventStream::Item) -> u64,
{
    pub fn new(calc_order: FOrd) -> Self {
        Join {
            calc_order,
            pipelines: Vec::new(),
            last_values: BinaryHeap::new(),
        }
    }

    pub fn add(&mut self, stream: EventStream) {
        self.pipelines.push(Box::pin(stream));
        self.last_values.push(QueueItem {
            event: None,
            order: 0,
            pipeline_index: self.pipelines.len() - 1,
        })
    }
}

impl<EventStream, FOrd> Stream for Join<EventStream, FOrd>
where
    EventStream: Stream,
    FOrd: Fn(&EventStream::Item) -> u64,
{
    type Item = EventStream::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>>
    {

        let JoinProj{calc_order, pipelines, last_values} = self.project();
        match last_values.peek() {
            Some(q_item) => {
                let index = q_item.pipeline_index;
                let pipe = pipelines[index].as_mut();
                let async_event = ready!(pipe.poll_next(cx)); //leave on error

                let old_event = last_values.pop().unwrap().event; //peek went ok already
                match async_event {
                    Some(new_event) => {
                        let key = (calc_order)(&new_event);
                        last_values.push(QueueItem {
                            event: Some(new_event),
                            order: key,
                            pipeline_index: index,
                        });
                    }
                    None => (), // stream is done - don't queue it again.
                };

                match old_event {
                    Some(event) => Poll::Ready(Some(event)),
                    None => self.poll_next(cx),
                }
            }
            None => Poll::Ready(None),
        }
    }
}
