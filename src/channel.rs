/* futures mpsc
 * 
 * test tests::async_stream         ... bench:      16,092 ns/iter (+/- 21)
 * test tests::async_stream_latency ... bench:           0 ns/iter (+/- 0)
 * test tests::async_stream_map10   ... bench:      16,066 ns/iter (+/- 15)
 * test tests::channel_buf1         ... bench:   6,645,534 ns/iter (+/- 14,848,741)
 * test tests::channel_buf1_latency ... bench:           0 ns/iter (+/- 0)
 * test tests::channel_buf2         ... bench:   4,284,739 ns/iter (+/- 10,485,849)
 * test tests::channel_buf2_chunk10 ... bench:   1,180,069 ns/iter (+/- 27,639)
 * test tests::channel_buf2_latency ... bench:           0 ns/iter (+/- 0)
 * test tests::fork_join            ... bench:   5,545,806 ns/iter (+/- 974,872)
 * test tests::fork_join_unorderd   ... bench:   4,299,225 ns/iter (+/- 971,332)
 * test tests::iter_stream          ... bench:       1,505 ns/iter (+/- 2)
 * test tests::measure_time         ... bench:         212 ns/iter (+/- 6)
 * test tests::shuffle              ... bench:   4,508,110 ns/iter (+/- 2,670,715)
 * test tests::spawn                ... bench:  17,275,048 ns/iter (+/- 1,600,796)
 * test tests::spawn_1              ... bench:  14,253,230 ns/iter (+/- 789,295)
 * test tests::sync_fn              ... bench:       5,503 ns/iter (+/- 8)
 * test tests::wait_task            ... bench:         494 ns/iter (+/- 2)
 *
 */

/* tokio mpsc
 *
 * test tests::async_stream         ... bench:      16,080 ns/iter (+/- 16)
 * test tests::async_stream_latency ... bench:           0 ns/iter (+/- 0)
 * test tests::async_stream_map10   ... bench:      16,049 ns/iter (+/- 17)
 * test tests::channel_buf1         ... bench:  42,521,164 ns/iter (+/- 2,883,418)
 * test tests::channel_buf1_latency ... bench:           0 ns/iter (+/- 0)
 * test tests::channel_buf2         ... bench:  19,494,587 ns/iter (+/- 16,999,064)
 * test tests::channel_buf2_chunk10 ... bench:   1,445,846 ns/iter (+/- 17,386)
 * test tests::channel_buf2_latency ... bench:           0 ns/iter (+/- 0)
 * test tests::fork_join            ... bench:   3,236,044 ns/iter (+/- 882,666)
 * test tests::fork_join_unorderd   ... bench:   4,046,818 ns/iter (+/- 1,297,440)
 * test tests::iter_stream          ... bench:       1,506 ns/iter (+/- 2)
 * test tests::measure_time         ... bench:         222 ns/iter (+/- 0)
 * ...
 */

pub use futures::channel::mpsc::{channel, Receiver, Sender};

/*

use futures::task::{Poll, Context};
use futures::sink::Sink;
use std::pin::Pin;
use pin_project::pin_project;


use tokio::sync::mpsc as tokio_channel;
use tokio_channel::error::{ClosedError, TrySendError};

pub use tokio_channel::Receiver;

#[pin_project(project = SenderProj)]
//#[derive(Clone)]
#[derive(Debug)]
pub struct Sender<I>(Option<tokio_channel::Sender<I>>);

fn tokio_sender_sink<I>(tx: tokio_channel::Sender<I>) 
    -> Sender<I> 
{
    Sender(Some(tx))
}

pub fn channel<I>(capacity: usize) 
    -> (Sender<I>, tokio_channel::Receiver<I>) 
{
    let (tx, rx) = tokio_channel::channel(capacity);
    (tokio_sender_sink(tx), rx)
}

impl<I> Sink<I> for Sender<I>
{
    type Error = ClosedError;

    fn poll_ready(
        self: Pin<&mut Self>, 
        cx: &mut Context) 
    -> Poll<Result<(), Self::Error>> 
    {
        let inner = self.project().0.as_mut().unwrap(); 
        let result = inner.poll_ready(cx);
        if result.is_pending() {
            eprintln!("is_pending: {:?}", inner);
        }

        result

    }

    fn start_send(self: Pin<&mut Self>, item: I) -> Result<(), Self::Error> {
        let inner = self.project().0.as_mut().unwrap(); 

        match inner.try_send(item) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_e)) => {
                panic!("channel full - poll_ready() must always be called before start_send().");
            },
            Err(TrySendError::Closed(_e)) => {
                unimplemented!("channel receiver closed. element is lost");
                //Err(ClosedError::new())
            }
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut Context) 
        -> Poll<Result<(), Self::Error>>
    {
        Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: Pin<&mut Self>,
        _cx: &mut Context) 
        -> Poll<Result<(), Self::Error>>
    {
        *self.project().0 = None;
        Poll::Ready(Ok(()))
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Sender(self.0.as_ref().map(|s| s.clone()))
    }
}
*/
