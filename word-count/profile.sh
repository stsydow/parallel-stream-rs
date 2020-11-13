#!/bin/bash
export RUSTFLAGS="-C force-frame-pointers -C target-cpu=native"
TEXT=./test_data/rand_text.txt
#TEXT=~/dev/test_data/100M_rand_text.txt

GProf2dot="./gprof2dot.py -f perf  --show-samples  --color-nodes-by-selftime"
PERF="perf record -F1000 --call-graph dwarf,4096 -D1"
#PERF="perf record --call-graph lbr -D1"
cargo build --release
#rustup run nightly perf record --call-graph lbr cargo wc-parallel

$PERF target/release/async-read $TEXT > /dev/null
perf script | c++filt | $GProf2dot  | dot -Tsvg -o async-read-output.svg

#$PERF target/release/wc-seq $TEXT > /dev/null
#perf script | c++filt | $GProf2dot  | dot -Tsvg -o seq-output.svg

#$PERF target/release/wc-seq-buf $TEXT > /dev/null
#perf script | c++filt | $GProf2dot  | dot -Tsvg -o seq-buf-output.svg

#$PERF target/release/wc-async-buf $TEXT > /dev/null
#perf script | c++filt | $GProf2dot  | dot -Tsvg -o async-buf-output.svg

#$PERF target/release/wc-parallel-chunked -t4 $TEXT > /dev/null
#perf script | c++filt | $GProf2dot | dot -Tsvg -o pipe-chunked-output.svg

#$PERF target/release/wc-parallel-buf -t4 $TEXT > /dev/null
#perf script | c++filt | $GProf2dot | dot -Tsvg -o pipe-buf-output.svg

#$PERF target/release/wc-parallel-partition-chunked -t4 $TEXT > /dev/null
#perf script | c++filt | $GProf2dot | dot -Tsvg -o para-chunked-output.svg

#$PERF target/release/wc-parallel-partition-buf -t4 $TEXT > /dev/null
#perf script | c++filt | $GProf2dot | dot -Tsvg -o para-buf-output.svg

#$PERF target/release/wc-parallel-partition-shuffle-chunked -t48 $TEXT > /dev/null
#perf script | c++filt | $GProf2dot | dot -Tsvg -o para-shuffle-output.svg

#$PERF target/release/wc-parallel-shuffle-new -t16 $TEXT > /dev/null
#perf script | c++filt | $GProf2dot | dot -Tsvg -o new-shuffle-output.svg

#run $T ./target/release/wc-parallel-partition-shuffle-chunked;
#	run $T ./target/release/wc-parallel-shuffle-new;
#	run $T ./target/release/wc-parallel-new;

#perf script | ./FlameGraph/stackcollapse-perf.pl | c++filt | ./FlameGraph/flamegraph.pl --width 4000 > flame.svg


