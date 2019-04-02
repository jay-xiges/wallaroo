# Copyright 2017 The Wallaroo Authors.
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

import datetime
import errno
import io
import logging
import threading
import time
import socket
import struct

from .errors import TimeoutError
from .logger import INFO2
from .stoppable_thread import StoppableThread

from wallaroo.experimental.connectors import (BaseIter,
                                              BaseSource,
                                              MultiSourceConnector)


try:
    basestring
except:
    basestring = (str, bytes)


class SingleSocketReceiver(StoppableThread):
    """
    Read length or newline encoded data from a socket and append it to an
    accumulator list.
    Multiple SingleSocketReceivers may write to the same accumulator safely,
    so long as they perform atomic writes (e.g. each append adds a single,
    complete entry).
    """

    __base_name__ = 'SingleSocketReceiver'

    def __init__(self, sock, accumulator, mode='framed', header_fmt='>I',
                 name=None):
        super(SingleSocketReceiver, self).__init__()
        self.sock = sock
        self.accumulator = accumulator
        self.mode = mode
        self.header_fmt = header_fmt
        self.header_length = struct.calcsize(self.header_fmt)
        if name:
            self.name = '{}:{}'.format(self.__base_name__, name)
        else:
            self.name = self.__base_name__

    def try_recv(self, bs, flags=0):
        """
        Try to to run `sock.recv(bs)` and return None if error
        """
        try:
            return self.sock.recv(bs, flags)
        except:
            return None

    def append(self, bs):
        if self.mode == 'framed':
            self.accumulator.append(bs)
        else:
            self.accumulator.append(bs + b'\n')

    def run(self):
        if self.mode == 'framed':
            self.run_framed()
        else:
            self.run_newlines()

    def run_newlines(self):
        data = []
        while not self.stopped():
            buf = self.try_recv(1024)
            if not buf:
                self.stop()
                if data:
                    self.append(b''.join(data))
            # We must be careful not to accidentally join two separate lines
            # nor split a line
            split = buf.split(b'\n')  # '\n' show as '' in list after split
            s0 = split.pop(0)
            if s0:
                if data:
                    data.append(s0)
                    self.append(b''.join(data))
                    data = []
                else:
                    self.append(s0)
            else:
                # s0 is '', so first line is a '\n', and overflow is a
                # complete message if it isn't empty
                if data:
                    self.append(b''.join(data))
                    data = []
            for s in split[:-1]:
                self.append(s)
            if split:  # not an empty list
                if split[-1]:  # not an empty string, i.e. it wasn't a '\n'
                    data.append(split[-1])
            time.sleep(0.000001)

    def run_framed(self):
        while not self.stopped():
            header = self.try_recv(self.header_length, socket.MSG_WAITALL)
            if not header:
                self.stop()
                continue
            expect = struct.unpack(self.header_fmt, header)[0]
            data = self.try_recv(expect, socket.MSG_WAITALL)
            if not data:
                self.stop()
            else:
                self.append(b''.join((header, data)))
                time.sleep(0.000001)

    def stop(self, *args, **kwargs):
        super(self.__class__, self).stop(*args, **kwargs)
        self.sock.close()


class TCPReceiver(StoppableThread):
    """
    Listen on a (host,port) pair and write any incoming data to an accumulator.

    If `port` is 0, an available port will be chosen by the operation system.
    `get_connection_info` may be used to obtain the (host, port) pair after
    `start()` is called.

    `max_connections` specifies the number of total concurrent connections
    supported.
    `mode` specifices how the receiver handles parsing the network stream
    into records. `'newlines'` will split on newlines, and `'framed'` will
    use a length-encoded framing, along with the `header_fmt` value (default
    mode is `'framed'` with `header_fmt='>I'`).

    You can read any data saved to the accumulator (a list) at any time
    by reading the `data` attribute of the receiver, although this attribute
    is only guaranteed to stop growing after `stop()` has been called.
    """
    __base_name__ = 'TCPReceiver'

    def __init__(self, host, port=0, max_connections=1000, mode='framed',
                 header_fmt='>I'):
        """
        Listen on a (host, port) pair for up to max_connections connections.
        Each connection is handled by a separate client thread.
        """
        super(TCPReceiver, self).__init__()
        self.host = host
        self.port = port
        self.address = '{}.{}'.format(host, port)
        self.max_connections = max_connections
        self.mode = mode
        self.header_fmt = header_fmt
        self.header_length = struct.calcsize(self.header_fmt)
        # use an in-memory byte buffer
        self.data = []
        # Create a socket and start listening
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.clients = []
        self.err = None
        self.event = threading.Event()
        self.start_time = None

    def get_connection_info(self, timeout=10):
        is_connected = self.event.wait(timeout)
        if not is_connected:
            raise TimeoutError("{} Couldn't get connection info after {}"
                               " seconds".format(self.__base_name__, timeout))
        return self.sock.getsockname()

    def run(self):
        self.start_time = datetime.datetime.now()
        try:
            self.sock.bind((self.host, self.port))
            self.sock.listen(self.max_connections)
            self.host, self.port = self.sock.getsockname()
            self.event.set()
            while not self.stopped():
                try:
                    (clientsocket, address) = self.sock.accept()
                except Exception as err:
                    try:
                        if self.stopped():
                            break
                        else:
                            raise err
                    except OSError as err:
                        if err.errno == errno.ECONNABORTED:
                            # [ECONNABORTED] A connection arrived, but it was
                            # closed while waiting on the listen queue.
                            # This happens on macOS during normal
                            # harness shutdown.
                            return
                        else:
                            logging.error("socket accept errno {}"
                                .format(err.errno))
                            self.err = err
                            raise
                cl = SingleSocketReceiver(clientsocket, self.data, self.mode,
                                          self.header_fmt,
                                          name='{}-{}'.format(
                                              self.__base_name__,
                                              len(self.clients)))
                logging.debug("{}:{} accepting connection from ({}, {}) on "
                             "port {}."
                             .format(self.__base_name__, self.name, self.host,
                                     self.port, address[1]))
                self.clients.append(cl)
                cl.start()
        except Exception as err:
            self.err = err
            raise

    def stop(self, *args, **kwargs):
        if not self.stopped():
            super(TCPReceiver, self).stop(*args, **kwargs)
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except OSError as err:
                if err.errno == errno.ENOTCONN:
                    # [ENOTCONN] Connection is already closed or unopened
                    # and can't be shutdown.
                    pass
                else:
                    raise
            self.sock.close()
            for cl in self.clients:
                cl.stop()

    def save(self, path, mode=None):
        c = 0
        with open(path, 'wb') as f:
            for item in self.data:
                f.write(item[:self.header_length])
                if mode == 'giles':
                    f.write(struct.pack('>Q', c))
                    c += 1
                f.write(item[self.header_length:])

    def bytes_received(self):
        return sum(map(len, self.data))


class Metrics(TCPReceiver):
    __base_name__ = 'Metrics'


class Sink(TCPReceiver):
    __base_name__ = 'Sink'


class Sender(StoppableThread):
    """
    Send length framed data to a destination (addr).

    `address` is the full address in the host:port format
    `reader` is a Reader instance
    `batch_size` denotes how many records to send at once (default=1)
    `interval` denotes the minimum delay between transmissions, in seconds
        (default=0.001)
    `header_length` denotes the byte length of the length header
    `header_fmt` is the format to use for encoding the length using
        `struct.pack`
    `reconnect` is a boolean denoting whether sender should attempt to
        reconnect after a connection is lost.
    """
    def __init__(self, address, reader, batch_size=1, interval=0.001,
                 header_fmt='>I', reconnect=False):
        logging.info("Sender({address}, {reader}, {batch_size}, {interval},"
            " {header_fmt}, {reconnect}) created".format(
                address=address, reader=reader, batch_size=batch_size,
                interval=interval, header_fmt=header_fmt,
                reconnect=reconnect))
        super(Sender, self).__init__()
        self.daemon = True
        self.reader = reader
        self.batch_size = batch_size
        self.batch = []
        self.interval = interval
        self.header_fmt = header_fmt
        self.header_length = struct.calcsize(self.header_fmt)
        self.address = address
        (host, port) = address.split(":")
        self.host = host
        self.port = int(port)
        self.name = 'Sender'
        self.error = None
        self._bytes_sent = 0
        self.reconnect = reconnect
        self.pause_event = threading.Event()
        self.data = []
        self.start_time = None

    def pause(self):
        self.pause_event.set()

    def paused(self):
        return self.pause_event.is_set()

    def resume(self):
        self.pause_event.clear()

    def send(self, bs):
        try:
            self.sock.sendall(bs)
        except OSError as err:
            if err.errno == 104 or err.errno == 54:
                # ECONNRESET on Linux or macOS, respectively
                is_econnreset = True
            else:
                is_econnreset = False
            logging.info("socket errno {} ECONNRESET {} stopped {}"
                .format(err.errno, is_econnreset, self.stopped()))
        self.data.append(bs)
        self._bytes_sent += len(bs)

    def bytes_sent(self):
        return self._bytes_sent

    def batch_append(self, bs):
        self.batch.append(bs)

    def batch_send(self):
        if len(self.batch) >= self.batch_size:
            self.batch_send_final()
            time.sleep(self.interval)

    def batch_send_final(self):
        if self.batch:
            self.send(b''.join(self.batch))
            self.batch = []

    def run(self):
        self.start_time = datetime.datetime.now()
        while not self.stopped():
            try:
                logging.info("Sender connecting to ({}, {})."
                             .format(self.host, self.port))
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.connect((self.host, self.port))
                while not self.stopped():
                    while self.paused():
                        # make sure to empty the send buffer before
                        #entering pause state!
                        self.batch_send_final()
                        time.sleep(0.001)
                    header = self.reader.read(self.header_length)
                    if not header:
                        self.maybe_stop()
                        break
                    expect = struct.unpack(self.header_fmt, header)[0]
                    body = self.reader.read(expect)
                    if not body:
                        self.maybe_stop()
                        break
                    self.batch_append(header + body)
                    self.batch_send()
                    time.sleep(0.000000001)
                self.batch_send_final()
                self.sock.close()
            except KeyboardInterrupt:
                logging.info("KeyboardInterrupt received.")
                self.stop()
                break
            except Exception as err:
                self.error = err
                logging.error(err)
            if not self.reconnect:
                break
            if not self.stopped():
                logging.info("Waiting 1 second before retrying...")
                time.sleep(1)

    def maybe_stop(self):
        if not self.batch:
            self.stop()

    def stop(self, *args, **kwargs):
        if not self.stopped():
            logging.log(INFO2, "Sender received stop instruction.")
        super(Sender, self).stop(*args, **kwargs)
        if self.batch:
            logging.warning("Sender stopped, but send buffer size is {}"
                            .format(len(self.batch)))

    def last_sent(self):
        if isinstance(self.reader.gen, MultiSequenceGenerator):
            return self.reader.gen.last_sent()
        else:
            raise ValueError("Can only use last_sent on a sender with "
                             "a MultiSequenceGenerator, or an ALOSender.")


class NoNonzeroError(ValueError):
    pass


def first_nonzero_index(seq):
    idx = 0
    for item in seq:
        if item == 0:
            idx += 1
        else:
            return idx
    else:
        raise NoNonzeroError("No nonzero values found in list")


class Sequence(object):
    def __init__(self, index, val=0):
        self.index = '{index:07d}'.format(index = index)
        self.key = self.index.encode()
        self.val = val

    def __next__(self):
        self.val += 1
        return (self.key, self.val)

    def __iter__(self):
        return self

    def next(self):
        return self.__next__()

    def throw(self, type=None, value=None, traceback=None):
        raise StopIteration


class MultiSequenceGenerator(object):
    """
    A growable collection of sequence generators.
    - Each new generator has its own partition
    - Messages are emitted in a round-robin fashion over the generator list
    - When a new generator joins, it takes over all new messages until it
      catches up
    - At stoppage time, all generators are allowed to reach the same final
      value
    """
    def __init__(self, base_index=0, initial_partitions=1, base_value=0):
        self._base_value = base_value
        self._next_index = base_index + initial_partitions
        self.seqs = [Sequence(x, self._base_value)
                     for x in range(base_index, self._next_index)]
        # self.seqs stores the last value sent for each sequence
        self._idx = 0  # the idx of the last sequence sent
        self._remaining = []
        self.lock = threading.Lock()

    def format_value(self, value, partition):
        return struct.pack('>IQ{}s'.format(len(partition)), 8+len(partition),
                           value, partition)

    def _next_value_(self):
        # Normal operation next value: round robin through the sets
        if self._idx >= len(self.seqs):
            self._idx = 0
        next_seq = self.seqs[self._idx]
        self._idx += 1
        return next(next_seq)

    def _next_catchup_value(self):
        # After stop() was called: all sets catch up to current max
        try:
            idx = first_nonzero_index(self._remaining)
            next_seq = self.seqs[idx]
            self._remaining[idx] -= 1
            return next(next_seq)
        except NoNonzeroError:
            # reset self._remaining so it can be reused
            if not self.max_val:
                self._remaining = []
            logging.debug("MultiSequenceGenerator: Stop condition "
                "reached. Final values are: {}".format(
                    self.seqs))
            self.throw()

    def add_sequence(self):
        if not self._remaining:
            logging.debug("MultiSequenceGenerator: adding new sequence")
            self.seqs.append(Sequence(self._next_index, self._base_value))
            self._next_index += 1

    def stop(self):
        logging.info("MultiSequenceGenerator: stop called")
        logging.debug("seqs are: {}".format(self.seqs))
        with self.lock:
            self.max_val = max([seq.val for seq in self.seqs])
            self._remaining = [self.max_val - seq.val for seq in self.seqs]
            logging.debug("_remaining: {}".format(self._remaining))

    def last_sent(self):
        return [('{}'.format(key), val) for (key,val) in
         [(seq.index, seq.val) for seq in self.seqs]]

    def send(self, ignored_arg):
        with self.lock:
            if self._remaining:
                idx, val = self._next_catchup_value()
            else:
                idx, val = self._next_value_()
        return self.format_value(val, idx)

    def throw(self, type=None, value=None, traceback=None):
        raise StopIteration

    def __iter__(self):
        return self

    def next(self):
        return self.__next__()

    def __next__(self):
        return self.send(None)

    def close(self):
        """Raise GeneratorExit inside generator.
        """
        try:
            self.throw(GeneratorExit)
        except (GeneratorExit, StopIteration):
            pass
        else:
            raise RuntimeError("generator ignored GeneratorExit")


def sequence_generator(stop=1000, start=0, header_fmt='>I', partition=''):
    """
    Generate a sequence of integers, encoded as big-endian U64.

    `stop` denotes the maximum value of the sequence (inclusive)
    `start` denotes the starting value of the sequence (exclusive)
    `header_length` denotes the byte length of the length header
    `header_fmt` is the format to use for encoding the length using
    `struct.pack`
    `partition` is a string representing the optional partition key. It is
    empty by default.
    """
    partition = partition.encode()
    size = 8 + len(partition)
    fmt = '>Q{}s'.format(len(partition)) if partition else '>Q'
    for x in range(start+1, stop+1):
        yield struct.pack(header_fmt, size)
        if partition:
            yield struct.pack(fmt, x, partition)
        else:
            yield struct.pack(fmt, x)


def iter_generator(items,
    to_bytes=lambda s: s.encode()
                       if isinstance(s, basestring) else str(s).encode(),
    header_fmt='>I',
    on_next=None):
    """
    Generate a sequence of length encoded binary records from an iterator.

    `items` is the iterator of items to encode
    `to_bytes` is a function for converting items to a bytes
    (default:`lambda s: s.encode() if isinstance(s, basestring) else
              str(s).encode()`)
    `header_fmt` is the format to use for encoding the length using
    `struct.pack`
    """
    for val in items:
        if on_next:
            on_next(val)
        byt = to_bytes(val)  # bite->bit, so byte->byt ;)
        yield struct.pack(header_fmt, len(byt))
        yield byt


def files_generator(files, mode='framed', header_fmt='>I', on_next=None):
    """
    Generate a sequence of binary data stubs from a set of files.

    - `files`: either a single filepath or a list of filepaths.
        The same filepath may be provided multiple times, in which case it will
        be read that many times from start to finish.
    - `mode`: 'framed' or 'newlines'. If 'framed' is used, `header_fmt` is
        used to determine how many bytes to read each time. Default: 'framed'
    - `header_fmt`: the format of the length encoding header used in the files
        Default: '>I'
    """
    if isinstance(files, basestring):
        files = [files]

    for path in files:
        if mode == 'newlines':
            for l in newline_file_generator(path):
                if on_next:
                    on_next(l)
                yield l
        elif mode == 'framed':
            for l in framed_file_generator(path, header_fmt):
                if on_next:
                    on_next(l)
                yield l
        else:
            raise ValueError("`mode` must be either 'framed' or 'newlines'")


def newline_file_generator(filepath, header_fmt='>I', on_next=None):
    """
    Generate length-encoded strings from a newline-delimited file.
    """
    with open(filepath, 'rb') as f:
        f.seek(0, 2)
        fin = f.tell()
        f.seek(0)
        while f.tell() < fin:
            o = f.readline().strip(b'\n')
            if o:
                if on_next:
                    on_next(o)
                yield struct.pack(header_fmt, len(o))
                yield o


def framed_file_generator(filepath, header_fmt='>I', on_next=None):
    """
    Generate length encoded records from a length-framed binary file.
    """
    header_length = struct.calcsize(header_fmt)
    with open(filepath, 'rb') as f:
        while True:
            header = f.read(header_length)
            if not header:
                break
            expect = struct.unpack(header_fmt, header)[0]
            body = f.read(expect)
            if not body:
                break
            if on_next:
                on_next(header + body)
            yield header
            yield body


class Reader(object):
    """
    A BufferedReader interface over a bytes generator
    """
    def __init__(self, generator):
        self.gen = generator
        self.overflow = b''

    def read(self, num):
        remaining = num
        out = io.BufferedWriter(io.BytesIO())
        remaining -= out.write(self.overflow)
        while remaining > 0:
            try:
                remaining -= out.write(next(self.gen))
            except StopIteration:
                break
        # first num bytes go to return, remainder to overflow
        out.seek(0)
        r = out.raw.read(num)
        self.overflow = out.raw.read()
        return r


class ALOSequenceGenerator(BaseIter, BaseSource):
    """
    A sequence generator with a resettable position.
    Starts at 1, and stops aftering sending `stop`.

    Usage: `ALOSequenceGenerator(partition, stop=1000, data=None)`
    if `data` is a list, data generated is appended to it in order
    as (position, value) tuples.
    """
    def __init__(self, key, stop=None, start=0):
        self.partition = key
        self.name = key.encode()
        self.key = key.encode()
        self.position = start
        self._stop = stop
        self.start = start
        self.data = []
        self.stopped = False

    def __str__(self):
        return ("ALOSequenceGenerator(partition: {}, stopped: {}, point_of_ref: {})"
                .format(self.name, self.stopped, self.point_of_ref()))

    def point_of_ref(self):
        return self.position

    def reset(self, pos=None):
        if pos is None:
            pos = self.start
        self.position = pos

    def __next__(self):
        # This has to be before the increment, otherwise point_of_ref()
        # doesn't return the previous position!
        if self.stopped:
            raise StopIteration
        if self._stop is not None:
            if self.position >= self._stop:
                raise StopIteration
        self.position += 1
        val, pos, key = (self.position, self.position, self.key)
        payload = struct.pack('>Q{}s'.format(len(key)), val, key)
        self.data.append(payload)
        return (payload, pos)

    def close(self):
        self.closed = True

    def stop(self):
        self.stopped = True


class ALOSender(StoppableThread):
    """
    A wrapper for MultiSourceConnector to look like a regular TCP Sender
    """
    def __init__(self, sources, version, cookie, program_name, instance_name,
                 addr):
        super(ALOSender, self).__init__()
        host, port = addr.split(':')
        port = int(port)
        self.client = client = MultiSourceConnector(
            version,
            cookie,
            program_name,
            instance_name,
            host, port)
        self.name = "ALOSender_{}".format("-".join(
            [source.partition for source in sources]))
        self.sources = sources
        logging.debug("ALO: sources = {}".format(sources))
        self.data = []
        for source in self.sources:
            source.data = self.data
        self.host = host
        self.port = port
        self.start_time = None
        self.error = None
        self.batch = [] # for compatibility with Sender during validations

    def run(self):
        self.start_time = datetime.datetime.now()
        self.client.connect()
        for source in self.sources:
            self.client.add_source(source)
        self.error = self.client.join()

    def stop(self, error=None):
        logging.debug("ALOSender stop")
        for source in self.sources:
            logging.debug("source to stop: {}".format(source))
            source.stop()
        if error is not None:
            self.client.shutdown(error=error)

    def pause(self):
        logging.debug("ALOSender pause: noop")

    def resume(self):
        logging.debug("ALOSender resume: noop")

    def last_sent(self):
        return [(source.partition, source.position) for source in self.sources]
