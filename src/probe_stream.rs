use futures::prelude::*;
use futures::task::Poll;
use core::pin::Pin;
use core::task::Context;
use std::time::{ Instant };
use futures::ready;
use crate::LogHistogram;

pub struct Tag<S>
{
    stream: S
}

impl<S> Tag<S>
{
    pub fn new(stream: S) -> Self {
        Tag {
            stream
        }
    }
}

impl<S, I>  Stream for Tag<S>
    where S: Stream<Item=I>
{
    type Item = (Instant, S::Item);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>>
    {
        let maybe_item = ready!(self.stream.poll_next(cx));
        Async::Ready( maybe_item.map(|item| (Instant::now(), item)))
    }
}

pub struct Meter<S>
{
    name: String,
    hist: LogHistogram,
    last_stamp: Option<Instant>,
    stream: S
}

impl<S> Meter<S>
{
    pub fn new(stream: S, name: String) -> Self {
        Meter {
            name,
            hist: LogHistogram::new(),
            last_stamp: None,
            stream
        }
    }
}

impl<S, I>  Stream for Meter<S>
    where S: Stream<Item=I>
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>>
    {
        let result = self.stream.poll_next(cx);
        let now = Instant::now();
        match result {
            Poll::Pending => {

            },
            Poll::Ready(None) => {
                self.hist.print_stats(&self.name);
            },
            Poll::Ready(Some(ref _item)) => {
                if let Some(last_time) = self.last_stamp {
                    let diff = now.duration_since(last_time).as_nanos() as u64;
                    self.hist.add_sample_ns(diff);
                }
                self.last_stamp = Some(now);
            }
        };

        result
    }
}

pub struct Probe<S>
{
    name: String,
    hist: LogHistogram,
    stream: S
}

impl<S, I> Probe<S>
    where S: Stream<Item=(Instant, I)>
{
    pub fn new(stream: S, name: String) -> Self {
        Probe {
            name,
            hist: LogHistogram::new(),
            stream
        }
    }
}

impl<S, I>  Stream for Probe<S>
    where S: Stream<Item=(Instant, I)>
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>>
    {
        let result = self.stream.poll_next(cx);

        match result {
            Poll::Pending => {

            },
            Poll::Ready(None) => {
                self.hist.print_stats(&self.name);
            },
            Poll::Ready(Some((ref time, ref _item))) => {
                self.hist.sample_now(time);
            }
        };

        result
    }
}

pub struct Map<S, F>
{
    stream: S,
    function: F
}

impl<S, F, Item, U> Map<S, F>
where S: Stream<Item=(Instant, Item)>,
      F: FnMut(Item) -> U,
{
    pub fn new(stream: S, function: F) -> Map<S, F>
    {
        Map {
            stream,
            function,
        }
    }
}

impl<S, F, I, U> Stream for Map<S, F>
where S: Stream<Item=(Instant, I)>,
      F: FnMut(I) -> U,
{
    type Item = (Instant, U);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<(Instant, U)>>
    {
        let option = ready!(self.stream.poll_next(cx));
        let result = option.map(|(timestamp, item)| (timestamp, (self.function)(item)) );
        Poll::Ready(result)
    }
}

//TODO ProbeAndTag?
