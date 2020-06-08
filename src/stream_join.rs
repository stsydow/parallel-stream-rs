use futures::{try_ready, Async, Poll, Stream};
use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::{Tag, ParallelStream};

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

pub struct JoinTagged<S, I>
where
    S: Stream<Item=Tag<I>>,
{
    pipelines: Vec<S>,
    last_values: BinaryHeap<TaggedQueueItem<I>>,
}

pub fn join_tagged<S, I>(par_stream: ParallelStream<S>) -> JoinTagged<S, I>
where S: Stream<Item=Tag<I>>
{
    let mut queue = BinaryHeap::new();
    for i in 0 .. par_stream.streams.len() {
        queue.push(TaggedQueueItem{event: None, source_id: i});
    }
    JoinTagged {
        pipelines: par_stream.streams,
        last_values: queue,
    }
}


impl<S, I> Stream for JoinTagged<S, I>
where
    S: Stream<Item=Tag<I>>,
{
    type Item = I;
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match self.last_values.peek() {
            Some(item) => {
                let index = item.source_id;
                let async_event = try_ready!(self.pipelines[index].poll()); //leave on error
                let item = self.last_values.pop().unwrap(); //peek went ok already
                match async_event {
                    Some(new_event) => {
                        let tagged_item = TaggedQueueItem{event: Some(new_event), source_id: item.source_id};
                        self.last_values.push(tagged_item);
                    },
                    None => (), // stream is done - don't queue it again.
                };

                match item.event {
                    Some(event) => Ok(Async::Ready(Some(event.untag()))),
                    None => self.poll(),
                }
            }
            None => {
                Ok(Async::Ready(None))
            },
        }
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

pub struct Join<EventStream, FOrd>
where
    EventStream: Stream, //where FOrd: Fn(&Event) -> u64
{
    calc_order: FOrd,
    pipelines: Vec<EventStream>,
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
        self.pipelines.push(stream);
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
    type Error = EventStream::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match self.last_values.peek() {
            Some(q_item) => {
                let index = q_item.pipeline_index;
                let async_event = try_ready!(self.pipelines[index].poll()); //leave on error

                let old_event = self.last_values.pop().unwrap().event; //peek went ok already
                match async_event {
                    Some(new_event) => {
                        let key = (self.calc_order)(&new_event);
                        self.last_values.push(QueueItem {
                            event: Some(new_event),
                            order: key,
                            pipeline_index: index,
                        });
                    }
                    None => (), // stream is done - don't queue it again.
                };

                match old_event {
                    Some(event) => Ok(Async::Ready(Some(event))),
                    None => self.poll(),
                }
            }
            None => Ok(Async::Ready(None)),
        }
    }
}
