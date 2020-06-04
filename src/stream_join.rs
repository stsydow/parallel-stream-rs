use futures::{try_ready, Async, Poll, Stream};
use std::cmp::Ordering;
use std::collections::BinaryHeap;

struct QueueItem<Event> {
    event: Option<Event>,
    order: u64,
    pipeline_index: usize,
}

impl<Event> Ord for QueueItem<Event> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.order.cmp(&other.order)
    }
}

impl<Event> PartialOrd for QueueItem<Event> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.order.partial_cmp(&other.order)
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
