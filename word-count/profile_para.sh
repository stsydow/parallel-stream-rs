export RUSTFLAGS="-C target-cpu=native"
#export RUST_BACKTRACE=1

#TEXT=./test_data/100M_rand_text.txt
#TEXT_ID=100M

#TEXT=./test_data/rand_text.txt
#TEXT_ID=600M

TEXT=./test_data/big_rand_text.txt
TEXT_ID=6G

#TEXT=./test_data/pico-text
#TEXT_ID=1024w6G

DATA_FILE="para.data"
RUNS=3
PERF="perf stat -r${RUNS} -x, -o ${DATA_FILE} --append -e task-clock -e duration_time,cycles,instructions,cache-misses,page-faults,context-switches,cpu-migrations"
# -x, -e mem_access

PICOWC=~/pico/build/examples/word-count/pico_wc

function run {
	CPUS=$1
	CPU_RANGE="0-$(($CPUS - 1))"
	BINARY=$2
	echo "BENCH: ${BINARY}-${CPUS}-${TEXT_ID}" >> $DATA_FILE
	taskset -c $CPU_RANGE $PERF $BINARY -t$1 $TEXT > /dev/null
}

function runpico {
	CPUS=$1
	CPU_RANGE="0-$(($CPUS - 1))"
	echo "BENCH: pico-wc-${CPUS}-${TEXT_ID}" >> $DATA_FILE
	export PARDEG=$CPUS
	taskset -c "0-$(($PARDEG - 1))" $PERF $PICOWC $TEXT  /dev/null
}

function runrustwp {
	CPUS=$1
	CPU_RANGE="0-$(($CPUS - 1))"
	echo "BENCH: rustwp-${CPUS}-${TEXT_ID}" >> $DATA_FILE
	taskset -c $CPU_RANGE $PERF ./rustwp/target/release/rustwp $CPUS $TEXT > /dev/null
}


cd rustwp
cargo build --release
cd ..

cargo build --release

RANGE="96 64 48 32 24 16 8 4 2 1"

for T in $RANGE
do
	#run $T ./target/release/wc-parallel-partition-shuffle-chunked;
	#run $T ./target/release/wc-parallel-partition-buf;
	#run $T ./pgo-wc-parallel-partition-shuffle-chunked;
	run $T ./target/release/wc-parallel-shuffle-new;
	#run $T ./target/release/wc-parallel-new;
	runpico $T;
	runrustwp $T;
done
run 1 ./target/release/wc-async-buf
#run 1 ./target/release/wc-seq-buf
echo "BENCH: wc-seq-c-1-${TEXT_ID}" >> $DATA_FILE
$PERF ./wc-seq-c $TEXT > /dev/null


