use tokio::prelude::*;
use tokio;
use std::time::{ Instant };
use futures::try_ready;
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
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let maybe_item = try_ready!(self.stream.poll());
        Ok(Async::Ready( maybe_item.map(|item| (Instant::now(), item))))
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
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let result = self.stream.poll();
        let now = Instant::now();
        match result {
            Err(ref _err) => {
                //error!("stream err!");
            }
            Ok(Async::NotReady) => {

            },
            Ok(Async::Ready(None)) => {
                self.hist.print_stats(&self.name);
            },
            Ok(Async::Ready(Some(ref _item))) => {
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
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let result = self.stream.poll();

        match result {
            Err(ref _err) => {
                //error!("stream err!");
            }
            Ok(Async::NotReady) => {

            },
            Ok(Async::Ready(None)) => {
                self.hist.print_stats(&self.name);
            },
            Ok(Async::Ready(Some((ref time, ref _item)))) => {
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
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Option<(Instant, U)>, S::Error> {
        let option = try_ready!(self.stream.poll());
        let result = match option {
            None => {
                None
            },
            Some((timestamp, item)) => {
                let f_result = (self.function)(item);
                Some((timestamp, f_result))
            }
        };

        Ok(Async::Ready(result))
    }
}

//TODO ProbeAndTag?
