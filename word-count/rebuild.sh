RUSTFLAGS="-C force-frame-pointers -C target-cpu=native"

cargo update
cargo build --release
cargo build
