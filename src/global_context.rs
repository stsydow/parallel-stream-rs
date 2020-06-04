use futures::{try_ready, Async, Poll, Stream};

pub struct GlobalContext<Ctx, InStream, F> {
    context: Ctx,
    input: InStream,
    work: F,
}

impl<E, R, Ctx, InStream, FWork> GlobalContext<Ctx, InStream, FWork>
where
    InStream: Stream<Item = E>,
    FWork: Fn(&mut Ctx, &E) -> R,
{
    pub fn new<F>(input: InStream, ctx_builder: F, work: FWork) -> Self
    where
        F: Fn() -> Ctx,
    {
        GlobalContext {
            context: ctx_builder(),
            input,
            work,
        }
    }
}

pub fn global_context<Event, R, Ctx, InStream, CtxInit, FWork>(
    input: InStream,
    ctx_builder: CtxInit,
    work: FWork,
) -> GlobalContext<Ctx, InStream, FWork>
where
    InStream: Stream<Item = Event>,
    CtxInit: Fn() -> Ctx,
    FWork: Fn(&mut Ctx, &Event) -> R,
{
    GlobalContext {
        context: ctx_builder(),
        input,
        work,
    }
}

/*
pub fn stream_context<E, R, InStream, Ctx> (input:InStream, initial_ctx: Ctx) -> GlobalContext<Ctx, InStream>
    where Ctx:Context<Event=E, Result=R>,
          InStream: Stream<Item=E, Error=EveError>
{
    GlobalContext::new(input, initial_ctx)
}
*/

impl<Event, Error, R, Ctx, InStream: Stream<Item = Event, Error = Error>, FWork> Stream
    for GlobalContext<Ctx, InStream, FWork>
where
    FWork: Fn(&mut Ctx, &Event) -> R,
{
    type Item = R;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let async_event = try_ready!(self.input.poll());
        let result = match async_event {
            Some(event) => {
                let result = (self.work)(&mut self.context, &event);
                Some(result)
            }
            None => None,
        };

        Ok(Async::Ready(result))
    }
}
