#export RUSTFLAGS="-C force-frame-pointers -C target-cpu=native --cfg stream_profiling"
export RUSTFLAGS="-C target-cpu=native --cfg stream_profiling"
#export RUSTFLAGS="-C target-cpu=native"
#RUST_BACKTRACE=1
#TEXT=~/dev/test_data/100M_rand_text.txt
TEXT=./test_data/rand_text.txt
#TEXT=./test_data/big_text.txt
#TEXT=~/word-count/test_data/pico-text

RUNS=1
#PERF="perf stat -r${RUNS} -e duration_time,task-clock,cycles,instructions,cache-misses,page-faults,context-switches,cpu-migrations"
PICOWC=~/pico/examples/word-count/pico_wc
PERF="time"

function run {
	CPUS=$1
	CPU_RANGE="0-$(($CPUS - 1))"
	BINARY=$2
	taskset -c $CPU_RANGE $PERF $BINARY -t$1 $TEXT > /dev/null
}

function runpico {
	CPUS=$1
	CPU_RANGE="0-$(($CPUS - 1))"
	export PARDEG=$CPUS
	echo "PARDEG=$PARDEG"
	taskset -c "0-$(($PARDEG - 1))" $PERF $PICOWC $TEXT  /dev/null
}

function runrustwp {
	CPUS=$1
	CPU_RANGE="0-$(($CPUS - 1))"
	taskset -c $CPU_RANGE $PERF ./rustwp/target/release/rustwp $CPUS $TEXT > /dev/null
}

THREADS=20

cd rustwp
cargo build --release
cd ..

cargo build --release -v

gcc -march=native -O3 wp.c -o wc-seq-c

#run 1 ./target/release/wc-parallel-partition-buf
#run 1 ./target/release/wc-async 
#$PERF ./wc-seq-c $TEXT >  /dev/null
RANGE="10 4 1"; 

run 1 ./target/release/wc-async-buf 
for T in $RANGE
do 
	run $T ./target/release/wc-parallel-partition-shuffle-chunked;
	#runpico $T;
	#runrustwp $T;
 	#run $T ./target/release/wc-parallel-partition-buf;	
done
run 1 ./target/release/wc-async 
#run 1 ./target/release/wc-seq-buf
#run 1 ./wc-seq-c 
exit

#$PERF ./wc-seq-c $TEXT>  /dev/null
#$PERF ./rustwp/target/release/rustwp $THREADS $TEXT > /dev/null

#$PERF ./target/release/wc-seq $TEXT > /dev/null
#$PERF ./target/release/wc-seq-buf $TEXT > /dev/null
$PERF ./target/release/wc-async $TEXT /dev/null
#$PERF ./target/release/wc-async-buf $TEXT > /dev/null
#$PERF ./target/release/wc-parallel $TEXT > /dev/null
#$PERF ./target/release/wc-parallel-fine $TEXT > /dev/null
#$PERF ./target/release/wc-parallel-chunked $TEXT > /dev/null
#$PERF ./target/release/wc-parallel-buf $TEXT > /dev/null
#$PERF ./target/release/wc-parallel-partition -t$THREADS $TEXT > /dev/null
#$PERF ./target/release/wc-parallel-partition-chunked -t$THREADS $TEXT #> /dev/null
$PERF ./target/release/wc-parallel-partition-buf -t$THREADS $TEXT #> /dev/null
#$PERF ./target/release/wc-parallel-partition-buf -t8 $TEXT > /dev/null
$PERF ./target/release/wc-parallel-partition-shuffle-chunked -t$THREADS $TEXT > /dev/null
#$PERF ./target/release/wc-timely $TEXT > /dev/null

#$PERF ~/dev/pico/build/examples/word-count/seq_wc $TEXT  /dev/null

export PARDEG=$THREADS
$PERF ~/pico/build/examples/word-count/pico_wc $TEXT  /dev/null

