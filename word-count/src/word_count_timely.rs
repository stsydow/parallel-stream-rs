use std::collections::HashMap;

use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Inspect, Map, Operator, Probe};
use timely::dataflow::{InputHandle, ProbeHandle};

use std::io::{self, Read};

use std::fs::File;
use std::hash::Hash;

use word_count::util::*;

fn hash_str(text: &str) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;

    let mut hasher = DefaultHasher::new();
    text.hash(&mut hasher);
    hasher.finish()
}

fn main() {
    // initializes and runs a timely dataflow.
    timely::execute(timely::Configuration::Process(4), |worker| {
        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        // define a distribution function for strings.
        //let exchange = Exchange::new(|x: &(String, i64)| hash_str(&x.0));
        let exchange = Exchange::new(|x: &(String, i64)| x.0.len() as u64);

        // create a new input, exchange data, and inspect its output
        worker.dataflow::<usize, _, _>(|scope| {
            input
                .to_stream(scope)
                .flat_map(|(text, diff): (String, i64)| {
                    text.split_whitespace()
                        .map(move |word| (word.to_owned(), diff))
                        .collect::<Vec<_>>()
                })
                .unary_frontier(exchange, "WordCount", |_capability, _info| {
                    let mut queues = HashMap::new();
                    let mut counts = HashMap::new();

                    move |input, output| {
                        while let Some((time, data)) = input.next() {
                            queues
                                .entry(time.retain())
                                .or_insert(Vec::new())
                                .push(data.replace(Vec::new()));
                        }

                        for (key, val) in queues.iter_mut() {
                            if !input.frontier().less_equal(key.time()) {
                                let mut session = output.session(key);
                                for mut batch in val.drain(..) {
                                    for (word, diff) in batch.drain(..) {
                                        let entry = counts.entry(word.clone()).or_insert(0i64);
                                        *entry += diff;
                                        session.give((word, *entry));
                                    }
                                }
                            }
                        }

                        queues.retain(|_key, val| !val.is_empty());
                    }
                })
                .inspect_time(|time, x| {
                    if time % 1000 == 0 {
                        println!("{}k: {:?}", time / 1000, x)
                    }
                })
                .probe_with(&mut probe);
        });

        if worker.index() == 0 {
            let conf = parse_args("word count dataflow");
            let mut buffer = String::new();
            if let Some(filename) = conf.input {
                File::open(&filename)
                    .expect(format!("can't open file \"{}\"", &filename).as_str())
                    .read_to_string(&mut buffer)
                    .expect(format!("can't read file \"{}\"", &filename).as_str());
            } else {
                io::stdin()
                    .read_to_string(&mut buffer)
                    .expect("can't read stdin");
            }

            let mut round: usize = 0;
            for word in buffer.split_ascii_whitespace() {
                input.send((word.to_owned(), 1));
                round = round + 1;
                input.advance_to(round);
                while probe.less_than(input.time()) {
                    worker.step();
                }
            }
            input.close();
        }
        /*
        // introduce data and watch!
        for round in 0..10 {
            input.send(("round".to_owned(), 1));
            input.advance_to(round + 1);
            while probe.less_than(input.time()) {
                worker.step();
            }
        }
        */
    })
    .unwrap();
}
