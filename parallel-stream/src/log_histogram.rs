use std::time::{ Instant };
use std::fmt;

pub struct OverheadEst {
    min_int: u64,
    //mean_int:u64,
    mean_ext:u64,
}

const fn no_overhead() -> OverheadEst {
    OverheadEst {
        min_int: 0,
        //mean_int: 0,
        mean_ext: 0,
    }
}

pub struct LogHistogram
{
    min:u64,
    max:u64,
    sum:u64,
    hist: [u64;65],
    overhead: OverheadEst,
    sample_ops:u64
}

const fn uncalibrated_hist() -> LogHistogram {
        LogHistogram {
            min: std::u64::MAX,
            max: 0,
            sum: 0,
            hist: [0;65], // use 128 bins maybe: 10^(log(1<<64 -1 ) / 128) = 1.41421356 or estimate with first two non zero bits
            overhead: no_overhead(),
            sample_ops: 0
        }
}

pub fn est_overhead() -> OverheadEst {
        const SAMPLE_SIZE:usize = 1024;
        let mut overhead_hist = uncalibrated_hist();

        /*
        for _i in 0 .. SAMPLE_SIZE {
            let ev_start_time_ov = Instant::now();
            for _j in 0 .. 100 {
                let _dummy = Instant::now();
            }
            overhead_hist.sample_now_chunk(100,&ev_start_time_ov);
        }
        let min_int_overhead = overhead_hist.min;
        let mean_int_overhead = (overhead_hist.sum + overhead_hist.size()/2)/ overhead_hist.size();

        overhead_hist.print_stats("internal overhead");

        overhead_hist = uncalibrated_hist();
        */

        let mut dummy = uncalibrated_hist();

        for _i in 0 .. SAMPLE_SIZE {
            let ev_start_time_ov = Instant::now();
            for _j in 0 .. 100 {
                let ev_start_time = Instant::now(); // ~25ns
                dummy.sample_now(&ev_start_time); // ~40ns
            }
            overhead_hist.sample_now_chunk(100, &ev_start_time_ov);
        }
        let mean_external_overhead = (overhead_hist.sum  + overhead_hist.size()/2) / overhead_hist.size();

        //overhead_hist.print_stats("gross overhead");

        //println!("expected ext. overhead: {}", format_nanos(mean_external_overhead as f32));

        OverheadEst {
            min_int: 15, //TODO estimation does not work // min_int_overhead,
            //mean_int: mean_int_overhead,
            mean_ext: mean_external_overhead
        }
}

const BARS: &'static [char;9] = &['_','▁','▂','▃','▄','▅','▆','▇','█'];
const BARS_MAX:usize = 8;

fn format_nanos(t:f32) -> String {

    if t < 500.0 {
        format!("{:0.3}ns", t)
    } else if t <  500_000.0 {
        format!("{:0.3}us", t/1000.0)
    } else if t < 500_000_000.0 {
        format!("{:0.3}ms", t/1000_000.0)
    } else {
        format!("{:0.3}s", t/1000_000_000.0)
    }
}



impl LogHistogram {

    pub fn new() -> Self {
        LogHistogram {
            min: std::u64::MAX,
            max: 0,
            sum: 0,
            hist: [0;65], // use 128 bins maybe: 10^(log(1<<64 -1 ) / 128) = 1.41421356 or estimate with first two non zero bits
            overhead: est_overhead(),
            sample_ops: 0
        }
    }

    pub fn add_sample_ns(&mut self, value: u64) {
        let corrected_val = value - self.overhead.min_int;
        self.sum += corrected_val;
        let t_max_n = corrected_val.max(self.max);
        self.max = t_max_n;
        let t_min_n = corrected_val.min(self.min);
        self.min = t_min_n;
        self.hist[(64 - corrected_val.leading_zeros()) as usize] += 1u64;
        self.sample_ops += 1;
    }

    pub fn add_sample_chunk_ns(&mut self, size:usize, chunk_value: u64) {
        if size > 0 {
            let corrected_val = chunk_value - self.overhead.min_int;
            let value = (corrected_val + (size/2) as u64) / size as u64;
            self.sum += corrected_val;
            let t_max_n = value.max(self.max);
            self.max = t_max_n;
            let t_min_n = value.min(self.min);
            self.min = t_min_n;
            self.hist[(64 - value.leading_zeros()) as usize] += size as u64;
        }
        self.sample_ops += 1;
    }

    // TODO use TSC for lower overhead:
    // https://crates.io/crates/tsc-timer
    // http://gz.github.io/rust-perfcnt/x86/time/fn.rdtsc.html
    pub fn sample_now(&mut self, ref_time: &Instant) {
                let difference = ref_time.elapsed()
                    .as_nanos() as u64;
        self.add_sample_ns(difference);
    }

    pub fn sample_now_chunk(&mut self, size: usize, ref_time: &Instant) {
                let difference = ref_time.elapsed()
                    .as_nanos() as u64;
        self.add_sample_chunk_ns(size, difference);
    }


    fn sparkline(& self) -> String {
        let mut spark_line:Vec<char> = Vec::with_capacity(64);
        {
            let f_max = self.hist.iter().max().unwrap();
            let log_f_max = 64 - f_max.leading_zeros() as i32;
            for i in 0 .. 64 {
                let bin_time = 1<<i;
                if self.min > bin_time ||  self.max * 2  < bin_time {
                    continue;
                }

                let f = self.hist[i];
                let log_f = 64 - f.leading_zeros() as i32;
                let b = if log_f_max > BARS_MAX as i32 {
                    log_f - (log_f_max - BARS_MAX as i32)
                } else {
                    log_f
                };
                if b < 0 {
                    if f > 0 {
                        spark_line.push('.');
                    } else {
                        spark_line.push(' ');
                    }
                } else {
                    spark_line.push(BARS[b as usize].clone());
                }
            }
        }
        spark_line.iter().collect::<String>()
    }

    pub fn print_stats(&self, name: &str) {
        let size = self.size();
        let mean_chunk_len = size as f32/ self.sample_ops as f32;
        let est_overhead = self.overhead.mean_ext * self.sample_ops;
        eprintln!("[{}] ops:{} ~chunk_len:{:0.1} ~overhead:{} time:{}\n~t_item:{}(med:{}) ~t_chunk:{}\n min: {} |{}| max: {}",
        name, self.sample_ops, mean_chunk_len,
        format_nanos(est_overhead as f32),
        format_nanos(self.sum as f32),

        format_nanos(self.sum as f32 / size as f32),
        format_nanos(self.percentile(0.5)),
        format_nanos(self.sum as f32 / self.sample_ops as f32),
        format_nanos(self.min as f32), self.sparkline() ,format_nanos(self.max as f32)
        );
    }

    fn size(&self) -> u64 {
        let mut n:u64 = 0;
        for i in 0 .. self.hist.len() {
            let f = self.hist[i];
            n += f;
        }
        n
    }

    // TODO log transformations for narrow distributions is inaccurate
    pub fn percentile(&self, p: f32) -> f32 {
        assert!(p >= 0.0 && p <= 1.0);

        let p_count = (self.size() as f32) * p;
        let mut samples:u64 = 0;
        for i in 0 .. self.hist.len() {
            let c_bin = self.hist[i];
            let samples_incl = samples + c_bin;
            if samples_incl > p_count as u64 {
                let d_bin = (p_count - samples as f32) / c_bin as f32;
                let log_val = (i -1) as f32 + d_bin;
                return log_val.exp2();
            }
            samples = samples_incl;
        }
        //unreachable!()
        return f32::NAN
    }

}

impl fmt::Debug for LogHistogram {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LogHistogram")
         .field("min", &self.min)
         .field("max", &self.max)
         .field("sum", &self.sum)
         .field("hist", &format_args!("{}", self.sparkline()))
         .finish()
    }
}

