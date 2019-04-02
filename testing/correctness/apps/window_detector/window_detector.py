#
# Copyright 2018 The Wallaroo Authors.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
#  implied. See the License for the specific language governing
#  permissions and limitations under the License.
#


import argparse
import datetime
import json
import time
import struct

import wallaroo


def application_setup(args):
    parser = argparse.ArgumentParser("Window Detector")
    parser.add_argument("--window-type", default="tumbling",
                        choices=["tumbling", "sliding", "counting"])
    parser.add_argument("--window-delay", type=int, default=0,
                        help=("Window delay"
                              "size in milliseconds. (Default: 0)"))
    parser.add_argument("--window-size", type=int, default=50,
                        help=("Window size in"
                              "milliseconds or units. (Default: 50)"))
    parser.add_argument("--window-slide", type=int, default=25,
                        help=("Window slide size, in milliseconds. "
                              "(Default: 25)"))
    parser.add_argument("--gen-source", action='store_true',
                    help="Use an internal source for resilience tests")
    parser.add_argument("--partitions", type=int, default=40,
                    help="Number of partitions for use with internal source")
    pargs, _ = parser.parse_known_args(args)

    if not '--cluster-initializer' in wallaroo._ARGS:
        pargs.partitions = 0

    source_name = "{} window".format(pargs.window_type)
    if pargs.gen_source:
        print("Using internal source generator")
        source = wallaroo.GenSourceConfig(source_name,
            MultiPartitionGenerator(pargs.partitions))
    else:
        print("Using TCP Source")
        in_name, in_host, in_port = wallaroo.tcp_parse_input_addrs(args)[0]
        source = wallaroo.TCPSourceConfig(in_host, in_port, in_name, decoder)

    p = wallaroo.source(source_name, source)
    p = p.key_by(extract_key)
    p = p.to(trace_id)
    p = p.key_by(extract_key)

    # Programmatically construct the window type and arguments
    if pargs.window_type == 'counting':
        print("Using window size: {} units".format(pargs.window_size))
        window = wallaroo.count_windows(pargs.window_size)
    else:
        print("Using window size: {} ms".format(pargs.window_size))
        window = wallaroo.range_windows(wallaroo.milliseconds(pargs.window_size))
        if pargs.window_delay:
            print("Using window_delay: {} ms".format(pargs.window_delay))
            window = window.with_delay(wallaroo.milliseconds(pargs.window_delay))
        if pargs.window_type == 'sliding':
            print("Using window_slide: {} ms".format(pargs.window_slide))
            window = window.with_slide(wallaroo.milliseconds(pargs.window_slide))
    # add the window to the topology
    p = p.to(window.over(Collect))

    p = p.to(split_accumulated)

    out_host, out_port = wallaroo.tcp_parse_output_addrs(args)[0]
    p = p.to_sink(wallaroo.TCPSinkConfig(out_host, out_port, encoder))
    return wallaroo.build_application("Tumbling Time Window Detector", p)


class MultiPartitionGenerator(object):
    """
    An internal message generator for use in resilience tests
    """
    def __init__(self, partitions=1):
        self.partitions = partitions

    def initial_value(self):
        if self.partitions == 0:
            return None
        return self.format_message(0,1)

    def apply(self, v):
        if self.partitions == 0:
            return None
        last_key = int(v.key)
        last_value = v.value
        if (last_key + 1) == self.partitions:
            next_value = last_value + 1
        else:
            next_value = last_value
        next_key = (last_key + 1) % self.partitions

        m = self.format_message(next_key, next_value)
        print("{} source decoded: {}".format(datetime.datetime.now(), m))
        return m

    def format_message(self, key, val):
        m = Message("{}".format(key), val)
        return m


@wallaroo.key_extractor
def extract_key(msg):
    return msg.key.split(".")[0]


class Message(object):
    def __init__(self, key, value):
        self.key = key
        self.value = value

    def __str__(self):
        return "({},{})".format(self.key, self.value)

    def __repr__(self):
        return str(self)


@wallaroo.computation(name="TraceID")
def trace_id(msg):
    print("trace_id({})".format(msg))
    return Message(msg.key + ".TraceID", msg.value)


class Collect(wallaroo.Aggregation):
    def initial_accumulator(self):
        return []

    def update(self, msg, accumulator):
        print("!@ Collect.update: append '", msg.key, "':", str(msg.value), "... appended to ", accumulator)
        # tag data key, then add it to accumulator
        accumulator.append(Message(msg.key + ".Collect", msg.value))

    def combine(self, accumulator1, accumulator2):
        new_acc = accumulator1 + accumulator2
        print("!@ Collect.combine:", accumulator1, " + ", accumulator2, " == ", new_acc)
        # return accumulator1 + accumulator2
        return new_acc

    def output(self, key, accumulator):
        keys = set(m.key for m in accumulator)
        values = tuple(m.value for m in accumulator)
        ts = time.time()
        print("Collect.output", ts, key, [str(m) for m in accumulator])
        assert(len(keys) <= 1)
        try:
            assert(keys.pop().split(".")[0] == key)
        except KeyError: # key set is empty because accumulator is empty
            return None
        return (key, values, ts)


@wallaroo.computation_multi(name="Split Accumulated")
def split_accumulated(data):
    key, values, ts = data
    return [(key, v, ts) for v in values]


@wallaroo.decoder(header_length=4, length_fmt=">I")
def decoder(bs):
    # Expecting a 64-bit unsigned int in big endian followed by a string
    val, key = struct.unpack(">Q", bs[:8])[0], bs[8:]
    key = key.decode("utf-8")  # python3 compat in downstream string concat
    print("decoder", key, val, time.time())
    return Message(key, val)


@wallaroo.encoder
def encoder(msg):
    print("encoder", time.time(), msg)
    s = json.dumps({'key': msg[0], 'value': msg[1], 'ts': msg[2]}).encode()
    return struct.pack(">I{}s".format(len(s)), len(s), s)
