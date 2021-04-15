use tokio::runtime::{self, Runtime};
use futures::{FutureExt, StreamExt};
use futures::executor::block_on;

use bencher::{benchmark_group, benchmark_main, Bencher};

const BLOCK_COUNT:usize = 1_000;

const DEV_ZERO: &'static str = "/dev/zero"; 
const BUFFER_SIZE:usize = 4096; 
//const BUFFER_SIZE:usize = 256; 
//const BUFFER_SIZE:usize = 16*4096; 

fn async_read_codec(b: &mut Bencher) {
    use tokio_util::codec::{BytesCodec, FramedRead, /*FramedWrite*/};
    use tokio::fs::File;
    let runtime = Runtime::new().expect("can not start runtime");

    b.iter(|| {
        let file_future = File::open("/dev/zero");
        let file: File = runtime
                .block_on(file_future)
                .expect("Can't open input file.");
        let input_stream = FramedRead::with_capacity(file , BytesCodec::new(), BUFFER_SIZE);
        let task = input_stream.take(BLOCK_COUNT).for_each(|_i| async {()});
        runtime.block_on(task);
    });

}

fn async_read_fs(b: &mut Bencher) {
    use async_fs::File;
    use futures::AsyncReadExt;
    let runtime = Runtime::new().expect("can not start runtime");

    b.iter(|| {
        let task = File::open("/dev/zero")
            .then(|r_file| async move {
                let mut file = r_file.expect("Can't open input file.");
                let mut buffer = [0u8; BUFFER_SIZE];

                for _i in 0 .. BLOCK_COUNT {
                    let count = file.read(&mut buffer).await
                        .expect("Can't read file.");
                    if count == 0 { break; }
                }
            });
       
        /*
        let input_stream = stream::unfold(file, |mut file| async move {
                let mut buffer = [0u8; BUFFER_SIZE];

                let count = file.read(&mut buffer[..]).await
                    .expect("Can't read file.");
                if count == 0 { 
                    return None;
                }
                assert_eq!(count, BUFFER_SIZE);
                let next_state = file;
                let yielded = buffer;
                Some( (yielded, next_state) )
        });
        let task = input_stream.take(BLOCK_COUNT).fold((), |(), ref _buffer | async {()});
        */
        runtime.block_on(task);
    });

}

fn smol_async_read_fs(b: &mut Bencher) {
    use async_fs::File;
    use futures::AsyncReadExt;

    b.iter(|| {
        let task = File::open("/dev/zero")
            .then(|r_file| async move {
                let mut file = r_file.expect("Can't open input file.");
                let mut buffer = [0u8; BUFFER_SIZE];

                for _i in 0 .. BLOCK_COUNT {
                    let count = file.read(&mut buffer).await
                        .expect("Can't read file.");
                    if count == 0 { break; }
                }
            });

        smol::block_on(task);
    });

}

fn exec_fut_async_read_fs(b: &mut Bencher) {
    use futures::executor::block_on;
    use async_fs::File;
    use futures::AsyncReadExt;

    b.iter(|| {
        let task = File::open("/dev/zero")
            .then(|r_file| async move {
                let mut file = r_file.expect("Can't open input file.");
                let mut buffer = [0u8; BUFFER_SIZE];

                for _i in 0 .. BLOCK_COUNT {
                    let count = file.read(&mut buffer).await
                        .expect("Can't read file.");
                    if count == 0 { break; }
                }
            });
        block_on(task);
    });
}


fn async_read(b: &mut Bencher) {
    use tokio::fs::{File};
    use tokio::io::{AsyncReadExt};
    let runtime = Runtime::new().expect("can not start runtime");

    b.iter(|| {
        let task = || async { 
                let mut file = File::open(DEV_ZERO)
                    .await
                    .unwrap();
                let mut buffer = [0u8; BUFFER_SIZE];

                for _i in 0 .. BLOCK_COUNT {
                    let count = file.read(&mut buffer)
                        .await
                        .unwrap();
                    if count == 0 { break; }
                }
            };

       
        runtime.block_on(task());
    });
}

fn async_read_std_file(b: &mut Bencher) {
    use std::fs::File;
    use std::io::Read;
    let runtime = Runtime::new().expect("can not start runtime");
    
    b.iter(|| {
        let task = || async { 
                let mut file = 
                    tokio::task::block_in_place(||
                    Box::pin(File::open(DEV_ZERO).unwrap())
                    )
                ;
                

                for _i in 0 .. BLOCK_COUNT {
                    let mut buffer = [0u8; BUFFER_SIZE];
                    let mut file_ref = file.as_mut();

                    tokio::task::block_in_place(move || {
                        file_ref.read_exact(&mut buffer).unwrap();
                    });
                    //yield buffer;
                }

            };
       
        runtime.block_on(task());
    });
}

fn sync_read(b: &mut Bencher) {
    use std::fs::File;
    use std::io::Read;
    
    b.iter(|| {
        let mut file = File::open(DEV_ZERO).expect("Unable to open file");

        let mut buffer = [0u8; BUFFER_SIZE];

        for _i in 0 .. BLOCK_COUNT {
            file.read_exact(&mut buffer)
                .expect("err reading file");
        }
    });
}
benchmark_group!(fs,
    async_read_codec,
    async_read_fs,
    async_read,
    async_read_std_file,
    smol_async_read_fs,
    exec_fut_async_read_fs,
    sync_read,
    );
benchmark_main!(fs);

