use futures::prelude::*;
use futures::task::Poll;
use core::pin::Pin;
use core::task::Context;
use std::time::{ Instant };
use futures::ready;
use pin_project::pin_project;
use crate::LogHistogram;

#[pin_project(project = TagProj)]
pub struct Tag<S>(#[pin] S);

impl<S> Tag<S>
{
    pub fn new(stream: S) -> Self {
        Tag(stream)
    }
}

impl<S, I>  Stream for Tag<S>
    where S: Stream<Item=I>
{
    type Item = (Instant, S::Item);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>>
    {
        let tag = self.project();
        let maybe_item = ready!(tag.0.poll_next(cx));
        Poll::Ready( maybe_item.map(|item| (Instant::now(), item)))
    }
}

#[pin_project(project = MeterProj)]
pub struct Meter<S>
{
    name: String,
    hist: LogHistogram,
    last_stamp: Option<Instant>,
#[pin]
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
        let MeterProj { name, hist, last_stamp, stream } = self.project();
        let result = stream.poll_next(cx);
        let now = Instant::now();
        match result {
            Poll::Pending => {

            },
            Poll::Ready(None) => {
                hist.print_stats(&name);
            },
            Poll::Ready(Some(ref _item)) => {
                if let Some(last_time) = last_stamp {
                    let diff = now.duration_since(*last_time).as_nanos() as u64;
                    hist.add_sample_ns(diff);
                }
                *last_stamp = Some(now);
            }
        };

        result
    }
}

#[pin_project(project = ProbeProj)]
pub struct Probe<S>
{
    name: String,
    hist: LogHistogram,
#[pin]
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
        let ProbeProj { name, hist, stream } = self.project();
        let result = stream.poll_next(cx);

        match result {
            Poll::Pending => {

            },
            Poll::Ready(None) => {
                hist.print_stats(&name);
            },
            Poll::Ready(Some((ref time, ref _item))) => {
                hist.sample_now(time);
            }
        };

        result
    }
}

#[pin_project(project = MapProj)]
pub struct Map<S, F>
{
#[pin]
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
        let MapProj { stream, function } = self.project();
        let option = ready!(stream.poll_next(cx));
        let result = option.map(|(timestamp, item)| (timestamp, (function)(item)) );
        Poll::Ready(result)
    }
}

//TODO ProbeAndTag?
