#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import re
from os.path import basename

from dateutil.parser import parse

OUT_FOLDER_DEFAULT = "data"
RE_EMPTY_COMMENT = re.compile(r'^(#.*)?\s*$')
RE_DATE = re.compile(r'^# started on (?P<date>.+)\s*$')
RE_BENCH = re.compile(r'^BENCH\:\s+(?P<experiment>\S+)-(?P<threads>\d+)-(?P<size>\w+)\s*$')
RE_VALUE = re.compile(r'^(?P<value>\S+),(?P<unit>\S*),(?P<name>\S+),(?P<variation>\S*),(?P<raw_value>\d+),.*$')

UNIT_SCALING = {
    'ns':1e-9,
    'msec':1e-3
}

def scale_by_unit(value, unit):
    factor = UNIT_SCALING.get(unit)
    assert factor
    return float(value) * factor

def main():

    if len(sys.argv) < 2:
        print("usage: "+ sys.argv[0] +" <perf_stat.csv> <out_folder> # else ./data/")
        sys.exit(1)

    out_folder = OUT_FOLDER_DEFAULT
    if len(sys.argv) > 2:
        out_folder = sys.argv[2]

    experiments = {}

    with open(sys.argv[1]) as perf_file:
        experiment_name = None
        threads = None


        for line in perf_file:
            date_match = RE_DATE.match(line)
            if date_match:
                fields = date_match.groupdict()
                experiments[experiment_name][threads]['date'] = parse(fields['date'])
                continue

            empty_match = RE_EMPTY_COMMENT.match(line)
            if empty_match:
                continue

            be_match = RE_BENCH.match(line)
            if be_match:

                fields = be_match.groupdict()
                experiment_name = basename(fields['experiment']) + '-' + fields['size']
                threads = int(fields['threads'])

                if not experiment_name in experiments:
                    experiments[experiment_name] = {}

                if not threads in experiments[experiment_name]:
                    experiments[experiment_name][threads] = {}
                continue

            v_match = RE_VALUE.match(line)
            if v_match:
                fields = v_match.groupdict()
                value_name = fields['name']
                unit = fields['unit']
                raw_value = fields['value']
                value = None
                if unit == '':
                    value = int(raw_value)
                else:
                    value = scale_by_unit(float(raw_value), unit)

                experiments[experiment_name][threads][value_name] = value
                continue

            print("can't parse: " + line)
            sys.exit(0)

    for ex_name in experiments:
        dat_file_name = "{}/{}.dat".format(out_folder, ex_name)
        with open(dat_file_name, "w") as f:
            experiment_set = experiments[ex_name]
            f.write("# {}\n".format(ex_name))
            f.write("threads\twalltime\tcputime\tspeedup\tdate\n")
            seq_time = experiment_set[1]['duration_time']
            for threads in experiment_set:
                row = experiment_set[threads]
                speedup = seq_time/row['duration_time']
                f.write("{:d}\t{}\t{}\t{}\t{}\n".format(threads, row['duration_time'],
                    row['task-clock'], speedup, row['date'].isoformat()))

if __name__ == "__main__":
    main()
