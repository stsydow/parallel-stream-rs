use crate::stream_fork::Fork;
use crate::stream_join::Join;
use futures::{
    future::join_all, lazy, stream::Stream, try_ready, Async, Future, IntoFuture, Poll, Sink,
    StartSend,
};
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{channel, Receiver, Sender};

const BUFFER_SIZE: usize = 4;

pub fn pipeline_task<InItem, OutItem, FBuildPipeline, OutStream>(
    src: Receiver<InItem>,
    sink: Sender<OutItem>,
    builder: impl AsRef<FBuildPipeline>,
) -> impl Future<Item = (), Error = ()>
where
    OutStream: Stream<Item = OutItem, Error = ()>,
    FBuildPipeline: Fn(Receiver<InItem>) -> OutStream,
{
    lazy(move || {
        let my_builder = builder.as_ref();

        let stream = my_builder(src);
        stream
            .forward(sink.sink_map_err(|e| panic!("send_err:{}", e)))
            .map(|(_stream, _sink)| ())
            .map_err(|e| panic!("pipe_err:{:?}", e))
    })
}

pub struct ParallelPipeline<PipelineStart, PipelineEnd, PipePair, FSel, FOrd, FBuildPipeline>
where
    PipelineStart: Sink,
    PipelineEnd: Stream,
{
    fork: Fork<PipelineStart, FSel>,
    join: Join<PipelineEnd, FOrd>,
    pipes: Vec<PipePair>,
    build_pipeline: Arc<FBuildPipeline>,
}

impl<Item, OutItem, OutStream, FSel, FOrd, FBuildPipeline>
    ParallelPipeline<
        Sender<Item>,
        Receiver<OutItem>,
        (Receiver<Item>, Sender<OutItem>),
        FSel,
        FOrd,
        FBuildPipeline,
    >
where
    OutStream: Stream<Item = OutItem, Error = ()> + Send + 'static,
    Item: Send + 'static,
    OutItem: Send + 'static, //TODO
    FSel: Fn(&Item) -> usize,
    FOrd: Fn(&OutItem) -> u64,
    FBuildPipeline: Fn(Receiver<Item>) -> OutStream + Send + Sync + 'static,
{
    pub fn new(
        selector: FSel,
        cmp: FOrd,
        build_pipeline: FBuildPipeline,
        pipe_count: usize,
    ) -> Self {
        let mut senders = Vec::new();
        let mut pipes = Vec::new();
        let mut join = Join::new(cmp);

        for _i in 0..pipe_count {
            let (in_tx, in_rx) = channel::<Item>(BUFFER_SIZE);
            let (out_tx, out_rx) = channel::<OutItem>(BUFFER_SIZE);
            senders.push(in_tx);
            pipes.push((in_rx, out_tx));
            join.add(out_rx);
        }

        let fork = Fork::new(selector, senders);

        ParallelPipeline {
            fork,
            join,
            pipes,
            build_pipeline: Arc::new(build_pipeline),
        }
    }

    /*
    fn tasks(&mut self) -> impl Iterator<Item=Future<Item=(), Error=()>> {
        let builder = self.build_pipeline.clone().as_ref();
        self.pipes.iter_mut().map(|(src, sink)| {
            pipeline_task(src, sink, builder)
        })
    }
    */

    fn run(self, runtime: &mut Runtime) {
        for (src, sink) in self.pipes {
            //Todo: we need a proper future for our stream builder - else the stream is send

            runtime.spawn(pipeline_task(src, sink, self.build_pipeline.clone()));
        }
    }
}

impl<PipelineStart, PipelineEnd, PipePair, FSel, FOrd, FBuildPipeline> Stream
    for ParallelPipeline<PipelineStart, PipelineEnd, PipePair, FSel, FOrd, FBuildPipeline>
where
    PipelineEnd: Stream,
    PipelineStart: Sink,
    FOrd: Fn(&PipelineEnd::Item) -> u64,
{
    type Item = PipelineEnd::Item;
    type Error = PipelineEnd::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.join.poll()
    }
}

impl<PipelineStart, PipelineEnd, PipePair, FSel, FOrd, FBuildPipeline> Sink
    for ParallelPipeline<PipelineStart, PipelineEnd, PipePair, FSel, FOrd, FBuildPipeline>
where
    PipelineEnd: Stream,
    PipelineStart: Sink,
    FSel: Fn(&PipelineStart::SinkItem) -> usize,
{
    type SinkItem = PipelineStart::SinkItem;
    type SinkError = PipelineStart::SinkError;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        self.fork.start_send(item)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        self.fork.poll_complete()
    }

    fn close(&mut self) -> Poll<(), Self::SinkError> {
        self.fork.close()
    }
}
