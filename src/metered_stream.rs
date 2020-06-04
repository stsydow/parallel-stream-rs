use tokio::prelude::*;
use tokio;
use tracing::{trace, error, Span};
use std::time::{ SystemTime};

pub struct Metered<S>
    where S: Stream
{
    span: Span,
    last_log: SystemTime,
    msg_count: usize,
    msg_per_sec: usize,
    stream: S
}


impl<S> Metered<S>
    where S: Stream
{
    pub fn new(stream: S, span: Span) -> Self {
        Metered {
            span,
            last_log: SystemTime::now(),
            msg_count: 0,
            msg_per_sec: 0,
            stream
        }
    }
}


impl<S>  Stream for Metered<S>
    where S: Stream
{
    type Item = S::Item;
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let _enter = self.span.enter();
        let result = self.stream.poll();

        match result {
            Err(ref _err) => {
                error!("stream err!");
            }
            Ok(Async::NotReady) => {

            },
            Ok(Async::Ready(None)) => {
                error!("finish");
            },
            Ok(Async::Ready(Some(ref _item))) => {
                let now = SystemTime::now();
                let difference = now.duration_since(self.last_log)
                    .expect("Clock may have gone backwards");
                if self.msg_count == 0 {
                    error!("start");
                }

                self.msg_count +=1;
                self.msg_per_sec += 1;
                if difference.as_secs() >= 1 {
                    error!(msg_count = self.msg_count, msg_per_sec = self.msg_per_sec);
                    self.msg_per_sec = 0;
                    self.last_log = now;
                }
                trace!(msg_count = self.msg_count);
            }
        };

        result
    }
}
