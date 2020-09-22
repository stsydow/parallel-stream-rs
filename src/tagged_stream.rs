use std::cmp::Ordering;
use std::pin::Pin;
use futures::ready;
use futures::Stream;
use futures::task::{Poll, Context};
use pin_project::{pin_project, project};

pub struct Tag<I> {
    seq_nr: usize,
    data: I
}

pub fn tag<I>(seq_nr: usize, data:I) -> Tag<I>
{
    Tag{seq_nr, data}
}

impl<I> Tag<I> {
    pub fn untag(self) -> I {
        self.data
    }

    pub fn nr(&self) -> usize { self.seq_nr }

    pub fn map<F, U>(self, f: F) -> Tag<U>
        where F:FnOnce(I) -> U
    {
        tag(self.seq_nr, f(self.data))
    }
}

impl<I> Ord for Tag<I> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.seq_nr.cmp(&other.seq_nr)
    }
}

impl<I> PartialOrd for Tag<I> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<I> PartialEq for Tag<I> {
    fn eq(&self, other: &Self) -> bool {
        self.seq_nr == other.seq_nr
    }
}

impl<I> Eq for Tag<I> {}

impl<I> AsRef<I> for Tag<I> {
    #[inline]
    fn as_ref(&self) -> &I {
        &self.data
    }
}

#[pin_project]
pub struct TaggedStream<S>
{
    #[pin]
    stream: S,
    seq_nr: usize
}

pub fn tagged_stream<S:Stream>(stream: S) -> TaggedStream<S>{
    TaggedStream{stream, seq_nr: 0usize}
}

impl<S:Stream> TaggedStream<S> {
    fn tag(&mut self, i:S::Item) -> Tag<S::Item> {
        let r = tag(self.seq_nr, i);
        self.seq_nr +=1;
        r
    }
}

impl<S:Stream> Stream for TaggedStream<S> {

    type Item = Tag<S::Item>;
    
    #[project]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>>
    {
        #[project]
        let TaggedStream { stream, seq_nr } = self.project();
        let option = ready!(stream.poll_next(cx));
        let result = match option {
            None => {
                None
            },
            Some(item) => {
                Some(self.tag(item))
            }
        };

        Poll::Ready(result)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}


#[pin_project]
pub struct Map<S, F>
{
    #[pin]
    stream: S,
    function: F
}

impl<S, F, I, U> Stream for Map<S, F>
where S: Stream<Item=Tag<I>>,
      F: FnMut(I) -> U,
{
    type Item = Tag<U>;

    #[project]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Tag<U>>>
    {
        #[project]
        let Map { stream, function } = self.project();
        let option = ready!(stream.poll_next(cx));
        let result = option.map(|t| t.map(&mut function));
        Poll::Ready(result)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

#[pin_project]
pub struct Untag<S>(
#[pin]
S
);

impl<S, I> Stream for Untag<S>
where S: Stream<Item=Tag<I>>,
{
    type Item = I;

    #[project]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<I>>
    {
        #[project]
        let Untag (stream) = self.project();
        let async_item = ready!(stream.poll_next(cx));
        let result = async_item.map(|t| t.untag());
        Poll::Ready(result)
    }
    
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

pub trait TaggedStreamExt<I>: Stream<Item=Tag<I>> {
    fn map<U, F>(self, f: F) -> Map<Self, F>
        where F: FnMut(I) -> U,
              Self: Sized
    {
        Map{stream: self, function: f}
    }

    fn untag(self) -> Untag<Self>
        where Self: Sized
    {
        Untag(self)
    }

    /*
    pub fn instrumented_map<U, F>(self, f: F, name:String)
        -> InstrumentedMap<Self, impl FnMut(Tag<I>) -> Tag<U> >
        where F: FnMut(I) -> U,
              //FTag: FnMut(Tag<I>) -> Tag<U>
    {
        let mut streams = Vec::new();
        for input in self.streams {
            //let map = input.instrumented_map(map_tag(f), name.clone());
            let map = input.instrumented_map(move |t| t.map(f) , name.clone());
            streams.push(map);
        }
        ParallelStream{ streams }
    }
    */
/*
    pub fn untag(self) -> impl Stream<Item=I>> {

        |t| t.untag()

    }
*/
}

