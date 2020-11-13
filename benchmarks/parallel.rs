use tokio::runtime::Runtime;
use futures::stream::{self, Stream, StreamExt};

use bencher::{benchmark_group, benchmark_main, Bencher};

use parallel_stream::channel;
use parallel_stream::fork_rr;
use parallel_stream::StreamExt as MyStreamExt;

const BLOCK_COUNT:usize = 1_000;

const THREADS:usize = 8;

const CHANNEL_BUFFER:usize = 100;

type Message = usize;
fn new_message(val:usize) -> Message {
    val
}

fn dummy_iter() -> impl Iterator<Item=Message> {
        (0 .. BLOCK_COUNT).map(|i| new_message(i))
}

fn dummy_stream() -> impl Stream<Item=Message> {
    stream::iter(
        dummy_iter()
    )
}

fn fork_join(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {

    let (fork, join) = {
        let mut senders = Vec::new();
        //let mut join = Join::new();
        let (out_tx, out_rx) = channel::<Message>(THREADS*CHANNEL_BUFFER);
        for _i in 0 .. THREADS {
            let (in_tx, in_rx) = channel::<Message>(CHANNEL_BUFFER);

            senders.push(in_tx);
            in_rx.forward_and_spawn(out_tx.clone(), &runtime);
        }

        let fork = fork_rr(senders);

        (fork, out_rx)
    };

    dummy_stream().forward_and_spawn(fork, &runtime);

    let recv_task = join
        .for_each(|_item| async {()});

        runtime.block_on(recv_task);
    });
}

fn fork_join_unorderd(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
            let pipeline = dummy_stream()
                .fork(THREADS, CHANNEL_BUFFER, &runtime)
                .join_unordered(CHANNEL_BUFFER*THREADS, &runtime);

            let pipeline_task = pipeline
            .for_each(|_item| async {()});
            
            runtime.block_on(pipeline_task);
    });
}

fn shuffle_1(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
            let pipeline = dummy_stream()
                .fork(THREADS, CHANNEL_BUFFER, &runtime)
                .shuffle_unordered(|i|{*i}, THREADS, CHANNEL_BUFFER, &runtime)
                .join_unordered(CHANNEL_BUFFER*THREADS, &runtime);

            let pipeline_task = pipeline
            .for_each(|_item| async {
                ()
            });
            
            runtime.block_on(pipeline_task);
        });
}

fn shuffle_unfair(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
            let pipeline = dummy_stream()
                .fork(THREADS, CHANNEL_BUFFER, &runtime)
                .shuffle_unordered(|_i|{0}, THREADS, CHANNEL_BUFFER, &runtime)
                .join_unordered(CHANNEL_BUFFER*THREADS, &runtime);

            let pipeline_task = pipeline
            .for_each(|_item| async {
                ()
            });
            
            runtime.block_on(pipeline_task);
        });
}

fn shuffle_nobuffer(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
            let pipeline = dummy_stream()
                .fork(THREADS, 1, &runtime)
                /*.map(|i| {
                    eprintln!("f{}", i);
                    i
                })*/
                .shuffle_unordered(|_i|{0}, THREADS, 1, &runtime)
                /*.map(|i| {
                    eprintln!("s{}", i);
                    i
                })*/
                .join_unordered(1, &runtime);

            let pipeline_task = pipeline
            .for_each(|_item| async {
                ()
            });
            
            runtime.block_on(pipeline_task);
        });
}

fn shuffle_random(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");
    b.iter(|| {
            let pipeline = dummy_stream()
                .fork(THREADS, CHANNEL_BUFFER, &runtime)
                .shuffle_unordered(|i|{i * 23}, THREADS, CHANNEL_BUFFER, &runtime)
                .join_unordered(CHANNEL_BUFFER*THREADS, &runtime);

            let pipeline_task = pipeline
            .for_each(|_item| async {
                ()
            });
            
            runtime.block_on(pipeline_task);
        });
}

fn fold_merge(b: &mut Bencher) {
    let runtime = Runtime::new().expect("can not start runtime");

    b.iter(|| {
        let pipeline_task = dummy_stream()
            .fork(THREADS, CHANNEL_BUFFER, &runtime)
            .instrumented_fold(|| 0usize, |acc: usize, _val:Message | async move {
                    acc + 1usize
            }, "acc".to_owned())
            .merge(0, |acc, part_acc| async move {
                acc + part_acc
            }, &runtime);
            
        let ops = runtime.block_on(pipeline_task);
        assert_eq!(ops, BLOCK_COUNT);
    });

}

benchmark_group!(parallel,
    fork_join,
    fork_join_unorderd,
    shuffle_1,
    shuffle_unfair,
    shuffle_nobuffer,
    shuffle_random,
    fold_merge,
    );

benchmark_main!(parallel);
