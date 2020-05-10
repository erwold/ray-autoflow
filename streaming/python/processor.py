import logging
import sys
import time
import types
import math
from collections import defaultdict
from ray.streaming.communication import _hash

logger = logging.getLogger(__name__)
logger.setLevel("INFO")


def _identity(element):
    return element


class ReadTextFile:
    """A source operator instance that reads a text file line by line.

    Attributes:
        filepath (string): The path to the input file.
    """

    def __init__(self, operator):
        self.filepath = operator.other_args
        # TODO (john): Handle possible exception here
        self.reader = open(self.filepath, "r")
        self.downstream_operator = None
        self.input_gate = None
        self.output_gate = None

    # Read input file line by line
    def run(self, input_gate, output_gate):
        while True:
            record = self.reader.readline()
            # Reader returns empty string ('') on EOF
            if not record:
                self.reader.close()
                return
            logger.info("source push {}".format(record[:-1]))
            output_gate.push(
                record[:-1])  # Push after removing newline characters

    def set_chaining(self, downstream_operator, input_gate, output_gate):
        self.downstream_operator = downstream_operator
        self.output_gate = output_gate

    def process(self, record):
        record = self.reader.readline()
        if not record:
            self.reader.close()
            return False
        #logger.info("source push {}".format(record[:-1]))
        if self.downstream_operator:
            self.downstream_operator.process(record[:-1])
        elif self.output_gate:
            self.output_gate.push(record[:-1])
        return True



class Map:
    """A map operator instance that applies a user-defined
    stream transformation.

    A map produces exactly one output record for each record in
    the input stream.

    """

    def __init__(self, operator):
        self.map_fn = operator.logic
        self.downstream_operator = None
        self.input_gate = None
        self.output_gate = None

    # Applies the mapper each record of the input stream(s)
    # and pushes resulting records to the output stream(s)
    def run(self, input_gate, output_gate):
        elements = 0
        while True:
            record = input_gate.pull()
            if record is None:
                return
            output_gate.push(self.map_fn(record))
            elements += 1

    def set_chaining(self, downstream_operator, input_gate, output_gate):
        self.downstream_operator = downstream_operator
        self.input_gate = input_gate
        self.output_gate = output_gate

    def process(self, record):
        if self.input_gate:
            record = self.input_gate.pull()
        if record is None:
            if self.downstream_operator:
                return self.downstream_operator.process(record)
            else:
                return False

        if record["event_type"] != "EventMarker":
            record = self.map_fn(record)
            record["event_type"] = "Bid"

        if self.downstream_operator:
            self.downstream_operator.process(record)
        else:
            self.output_gate.push(record)
        return True


class FlatMap:
    """A map operator instance that applies a user-defined
    stream transformation.

    A flatmap produces one or more output records for each record in
    the input stream.

    Attributes:
        flatmap_fn (function): The user-defined function.
    """

    def __init__(self, operator):
        self.flatmap_fn = operator.logic
        self.downstream_operator = None
        self.input_gate = None
        self.output_gate = None

    # Applies the splitter to the records of the input stream(s)
    # and pushes resulting records to the output stream(s)
    def run(self, input_gate, output_gate):
        while True:
            record = input_gate.pull()
            if record is None:
                return
            logger.info("flatmap {}".format(self.flatmap_fn(record)))
            output_gate.push_all(self.flatmap_fn(record))

    def set_chaining(self, downstream_operator, input_gate, output_gate):
        self.downstream_operator = downstream_operator
        self.input_gate = input_gate
        self.output_gate = output_gate

    def process(self, record):
        if self.input_gate:
            record = self.input_gate.pull()
        if record is None:
            if self.downstream_operator:
                return self.downstream_operator.process(record)
            else:
                return False
        #logger.info("flatmap {}".format(record))
        records = self.flatmap_fn(record)

        #logger.info("after flatmap {}".format(records))
        for record in records:
            if self.downstream_operator:
                self.downstream_operator.process(record)
            else:
                self.output_gate.push(record)
        return True


class Filter:
    """A filter operator instance that applies a user-defined filter to
    each record of the stream.

    Output records are those that pass the filter, i.e. those for which
    the filter function returns True.

    Attributes:
        filter_fn (function): The user-defined boolean function.
    """

    def __init__(self, operator):
        self.filter_fn = operator.logic
        self.downstream_operator = None
        self.input_gate = None
        self.output_gate = None

    # Applies the filter to the records of the input stream(s)
    # and pushes resulting records to the output stream(s)
    def run(self, input_gate, output_gate):
        while True:
            record = input_gate.pull()
            if record is None:
                return
            if self.filter_fn(record):
                output_gate.push(record)

    def set_chaining(self, downstream_operator, input_gate, output_gate):
        self.downstream_operator = downstream_operator
        self.input_gate = input_gate
        self.output_gate = output_gate

    def process(self, record):
        if self.input_gate:
            record = self.input_gate.pull()
        if record is None:
            if self.downstream_operator:
                return self.downstream_operator.process(record)
            else:
                return False
        record = self.filter_fn(record)

        if self.downstream_operator:
            self.downstream_operator.process(record)
        else:
            self.output_gate.push(record)
        return True

class Sink:

    def __init__(self, operator):
        self.sink = operator.logic
        self.downstream_operator = None
        self.input_gate = None
        self.output_gate = None

    def set_chaining(self, downstream_operator, input_gate, output_gate):
        self.downstream_operator = downstream_operator
        self.input_gate = input_gate
        self.output_gate = output_gate

    def process(self, record):
        if self.input_gate:
            record = self.input_gate.pull()
        if record is None:
            logger.info("Throughput: {}".format(self.sink.throughputs))
            logger.info("Latency: {}".format(self.sink.latencies))
            return False

        result = self.sink.evict(record)
        #if result is not None:
        #    print("latency: {}".format(result))

        # if self.downstream_operator:
        #    self.downstream_operator.process(record)
        # elif self.output_gate:
        #    self.output_gate.push(record)
        return True



class Inspect:
    """A inspect operator instance that inspects the content of the stream.
    Inspect is useful for printing the records in the stream.
    """

    def __init__(self, operator):
        self.inspect_fn = operator.logic
        self.downstream_operator = None
        self.input_gate = None
        self.output_gate = None

    def run(self, input_gate, output_gate):
        # Applies the inspect logic (e.g. print) to the records of
        # the input stream(s)
        # and leaves stream unaffected by simply pushing the records to
        # the output stream(s)
        while True:
            record = input_gate.pull()
            if record is None:
                return
            if output_gate:
                output_gate.push(record)
            self.inspect_fn(record)

    def set_chaining(self, downstream_operator, input_gate, output_gate):
        self.downstream_operator = downstream_operator
        self.input_gate = input_gate
        self.output_gate = output_gate

    def process(self, record):
        if self.input_gate:
            record = self.input_gate.pull()
        if record is None:
            if self.downstream_operator:
                return self.downstream_operator.process(record)
            else:
                return False
        record = self.inspect_fn(record)

        if self.downstream_operator:
            self.downstream_operator.process(record)
        elif self.output_gate:
            self.output_gate.push(record)
        return True


class Reduce:
    """A reduce operator instance that combines a new value for a key
    with the last reduced one according to a user-defined logic.
    """

    def __init__(self, operator):
        self.reduce_fn = operator.logic
        # Set the attribute selector
        self.attribute_selector = operator.other_args
        if self.attribute_selector is None:
            self.attribute_selector = _identity
        elif isinstance(self.attribute_selector, int):
            self.key_index = self.attribute_selector
            self._attribute_selector =\
                lambda record: record[self.attribute_selector]
        elif isinstance(self.attribute_selector, str):
            self._attribute_selector =\
                lambda record: vars(record)[self.attribute_selector]
        elif not isinstance(self.attribute_selector, types.FunctionType):
            sys.exit("Unrecognized or unsupported key selector.")
        self.state = {}  # key -> value
        self.downstream_operator = None
        self.input_gate = None
        self.output_gate = None

    # Combines the input value for a key with the last reduced
    # value for that key to produce a new value.
    # Outputs the result as (key,new value)
    def run(self, input_gate, output_gate):
        while True:
            record = input_gate.pull()
            if record is None:
                return
            key, rest = record
            #logger.info("reduce {}".format(record))
            new_value = self._attribute_selector(rest)
            # TODO (john): Is there a way to update state with
            # a single dictionary lookup?
            try:
                old_value = self.state[key]
                new_value = self.reduce_fn(old_value, new_value)
                self.state[key] = new_value
            except KeyError:  # Key does not exist in state
                self.state.setdefault(key, new_value)
            output_gate.push((key, new_value))

    def set_chaining(self, downstream_operator, input_gate, output_gate):
        self.downstream_operator = downstream_operator
        self.input_gate = input_gate
        self.output_gate = output_gate

    def process(self, record):
        if self.input_gate:
            record = self.input_gate.pull()
        if record is None:
            if self.downstream_operator:
                return self.downstream_operator.process(record)
            else:
                return False

        key, rest = record
        new_value = self._attribute_selector(rest)
        try:
            old_value = self.state[key]
            new_value = self.reduce_fn(old_value, new_value)
            self.state[key] = new_value
        except KeyError:  # Key does not exist in state
            self.state.setdefault(key, new_value)

        if self.downstream_operator:
            self.downstream_operator.process((key, new_value))
        else:
            self.output_gate.push((key, new_value))
        return True

    # Returns the state of the actor
    def get_state(self):
        return self.state


class LocalReduce:
    """A local reduce operator instance that behave like the
    same as reduce operator except it keeps only the partial
    state, and push result according to its flushing policy
    and when it flushes, it will garbage-collect the flushed
    state
    """

    def __init__(self, operator):
        self.reduce_fn = operator.logic
        # Set the attribute selector
        self.attribute_selector = operator.other_args
        if self.attribute_selector is None:
            self.attribute_selector = _identity
        elif isinstance(self.attribute_selector, int):
            self.key_index = self.attribute_selector
            self._attribute_selector =\
                lambda record: record[self.attribute_selector]
        elif isinstance(self.attribute_selector, str):
            self._attribute_selector =\
                lambda record: vars(record)[self.attribute_selector]
        elif not isinstance(self.attribute_selector, types.FunctionType):
            sys.exit("Unrecognized or unsupported key selector.")
        self.state = {}  # key -> value
        self.downstream_operator = None
        self.input_gate = None
        self.output_gate = None

        self.output_channel = {}
        self.num_process_list = []
        self.state_list = []
        #todo: can adjust due to real environment
        self.watermark_list = []
        self.num_channel = 0

    def init(self):
        channels = self.output_gate.get_keyby_channels()
        for channel in channels:
            self.output_channel[self.num_channel] = channel.str_qid
            self.state_list.append({})
            self.num_process_list.append(0)
            self.watermark_list.append(500)
            self.num_channel = self.num_channel + 1


    def set_chaining(self, downstream_operator, input_gate, output_gate):
        assert output_gate is not None
        self.downstream_operator = downstream_operator
        self.input_gate = input_gate
        self.output_gate = output_gate
        self.init()

    def process(self, record):
        # note: actually, There shouldn't be any operator after this
        if self.input_gate:
            record = self.input_gate.pull()
        if record is None:
            # push all left to remote downstream operator
            for index, channel_id in self.output_channel.items():
                self.output_gate.push_record_to_channel(channel_id, self.state_list[index])
                self.state_list[index].clear()
            return False

        key, rest = record
        h = _hash(key)
        index = h % self.num_channel

        new_value = self._attribute_selector(rest)
        try:
            old_value = self.state_list[index][key]
            new_value = self.reduce_fn(old_value, new_value)
            self.state_list[index][key] = new_value
        except KeyError:  # Key does not exist in state
            self.state_list[index].setdefault(key, new_value)

        self.num_process_list[index] = self.num_process_list[index] + 1
        if self.num_process_list[index] == self.watermark_list[index]:
            # LocalReduce operator shouldn't have a local downstream operator
            qid = self.output_channel[index]
            self.output_gate.push_record_to_channel(qid, self.state_list[index])
            self.state_list[index].clear()
            self.num_process_list[index] = 0
            # todo: modify watermark
            logger.info("[LPQInfo] get_channel_ratio: {}".format(
                self.output_gate.get_channel_ratio(self.output_channel[index])
            ))

        # if self.downstream_operator:
        #    self.downstream_operator.process((key, new_value))
        # else:
        #    self.output_gate.push((key, new_value))
        return True

    # Returns the state of the actor
    def get_state(self):
        return self.state


class KeyBy:
    """A key_by operator instance that physically partitions the
    stream based on a key.
    """

    def __init__(self, operator):
        # Set the key selector
        self.key_selector = operator.other_args
        if isinstance(self.key_selector, int):
            self._key_selector = lambda r: r[self.key_selector]
        elif isinstance(self.key_selector, str):
            #self._key_selector = lambda record: vars(record)[self.key_selector]
            self._key_selector = lambda record: record[self.key_selector]
        elif not isinstance(self.key_selector, types.FunctionType):
            sys.exit("Unrecognized or unsupported key selector.")
        self.downstream_operator = None
        self.input_gate = None
        self.output_gate = None

    # The actual partitioning is done by the output gate
    def run(self, input_gate, output_gate):
        while True:
            record = input_gate.pull()
            if record is None:
                return
            logger.info("keyby record {}".format(record))
            key = self._key_selector(record)
            logger.info("key {}".format(key))
            output_gate.push((key, record))

    def set_chaining(self, downstream_operator, input_gate, output_gate):
        self.downstream_operator = downstream_operator
        self.input_gate = input_gate
        self.output_gate = output_gate

    def process(self, record):
        if self.input_gate:
            record = self.input_gate.pull()
        if record is None:
            if self.downstream_operator:
                return self.downstream_operator.process(record)
            else:
                return False
        #logger.info("keyby record {}".format(record))
        key = self._key_selector(record)
        #logger.info("after keyby record {}".format((key, record)))

        if self.downstream_operator:
            self.downstream_operator.process((key, record))
        else:
            self.output_gate.push((key, record))
        return True



class EventTimeWindow:
    """An event time window operator instance (tumbling or sliding)"""

    def __init__(self, operator):
        assert len(operator.other_args) == 3
        self.window_length_ms = operator.other_args[0]
        self.slide_ms = operator.other_args[1]
        self.aggregator = operator.logic
        self.state = {}
        self.offset = operator.other_args[2]
        self.range = math.trunc(math.ceil(
                                self.window_length_ms / self.slide_ms))
        self.downstream_operator = None
        self.input_gate = None
        self.output_gate = None

    def set_chaining(self, downstream_operator, input_gate, output_gate):
        self.downstream_operator = downstream_operator
        self.input_gate = input_gate
        self.output_gate = output_gate

    def collect_expired_windows(self, event_time):
        result = []
        min_open_window = (event_time // self.slide_ms) - self.range + 1
        indexes = []
        for w, window in self.state.items():
            if w < min_open_window:
                if self.aggregator is None:
                    for record in window:
                        record.window = w
                        record.system_time = event_time
                    result += window
                else:
                    for auction, count in window.items():
                        record = {
                            "auction": auction,
                            "count": count,
                            "window": w,
                            "system_time": event_time,
                            "event_type": "Record",
                        }
                        result.append(record)
                indexes.append(w)
                logger.info("Firing window {} on time {}".format(w, event_time))
        for i in indexes:
            self.state.pop(i)
        return result

    def __find_windows(self, record):
        windows = []
        event_time = record["dateTime"]
        slot = -1
        slot = event_time // self.slide_ms
        window_end = (slot * self.slide_ms) + self.window_length_ms
        if event_time > window_end:
            return windows
        min = slot - self.range + 1
        min_window_id = min if min >= self.offset else self.offset
        max_window_id = slot if slot >= self.offset else self.offset
        windows = list(range(min_window_id, max_window_id + 1))
        return windows

    def __assigner(self, record):
        windows = self.__find_windows(record)
        if self.aggregator is not None:
            for w in windows:
                if w not in self.state:
                    self.state[w] = self.aggregator.initialize_window()
                self.aggregator.update(self.state[w], record)
        else:
            for w in windows:
                if w not in self.state:
                    self.state[w] = []
                self.state[w].append(record)

    def process(self, record):
        # todo: Flush the rest state when EOF reach?
        if self.input_gate:
            record = self.input_gate.pull()
        if record is None:
            if self.downstream_operator:
                return self.downstream_operator.process(record)
            else:
                return False

        self.__assigner(record)
        windows = self.collect_expired_windows(record["dateTime"])

        if len(windows) > 0:
            if self.downstream_operator:
                # todo: if there is downstream operator, should we split them?
                self.downstream_operator.process(windows)
            else:
                self.output_gate.push_all(windows)

        return True


class TimeWindowJoin:
    """An event time window join operator instance that join two streams."""
    def __init__(self, operator):
        assert len(operator.other_args) == 3
        self.window_length_ms = operator.other_args[0]
        self.slide_ms = operator.other_args[1]
        self.join_logic = operator.logic
        assert(self.join_logic is not None), (self.join_logic)
        self.state = {}
        self.offset = operator.other_args[2]
        self.range = math.trunc(math.ceil(
                                self.window_length_ms / self.slide_ms))
        self.downstream_operator = None
        self.input_gate = None
        self.output_gate = None

    def set_chaining(self, downstream_operator, input_gate, output_gate):
        self.downstream_operator = downstream_operator
        self.input_gate = input_gate
        self.output_gate = output_gate

    def expired_left_windows(self, event_time):
        while len(self.join_logic.left_state) > 0 and self.join_logic.left_state[0]["dateTime"] < event_time:
            record = self.join_logic.left_state.pop(0)
            logger.info("Firing left_state record {} on time {}".format(record, event_time))

    def expired_right_windows(self, event_time):
        while len(self.join_logic.right_state) > 0 and self.join_logic.right_state[0]["dateTime"] < event_time:
            record = self.join_logic.right_state.pop(0)
            logger.info("Firing right_state record {} on time {}".format(record, event_time))

    def __find_windows(self, record):
        event_time = record["dateTime"]
        slot = -1
        slot = event_time // self.slide_ms
        window_start = (slot - self.range + 1) * self.slide_ms
        window_end = (slot * self.slide_ms) + self.window_length_ms
        if event_time > window_end:
            return []
        return [window_start, window_end]

    def process(self, record):
        # todo: Flush the rest state when EOF reach?
        if self.input_gate:
            records = self.input_gate.pull()
        if records is None:
            if self.downstream_operator:
                return self.downstream_operator.process(records)
            else:
                return False

        from_which, record = records
        window_range = self.__find_windows(record)
        if from_which is "left":
            self.expired_right_windows(window_range[0])
            self.process_logic = self.join_logic.process_left
        else:
            self.expired_left_windows(window_range[0])
            self.process_logic = self.join_logic.process_right

        results = self.process_logic(window_range, record)

        if len(results) > 0:
            if self.downstream_operator:
                # todo: if there is downstream operator, should we split them?
                self.downstream_operator.process(results)
            else:
                self.output_gate.push_all(results)

        return True


class Join:
    """A join operator instance that join two streams."""

    def __init__(self, operator):
        self.join_logic = operator.logic
        assert(self.join_logic is not None), (self.join_logic)
        self.process_logic = None

        self.downstream_operator = None
        self.input_gate = None
        self.output_gate = None

    def set_chaining(self, downstream_operator, input_gate, output_gate):
        self.downstream_operator = downstream_operator
        self.input_gate = input_gate
        self.output_gate = output_gate

    def process(self, record):
        # todo: actually, join operator shouldn't be chained
        if self.input_gate:
            records = self.input_gate.pull()
        if records is None:
            if self.downstream_operator:
                return self.downstream_operator.process(records)
            else:
                return False

        from_which, record = records
        if from_which is "left":
            self.process_logic = self.join_logic.process_left
        else:
            self.process_logic = self.join_logic.process_right

        records = self.process_logic(record)

        if self.downstream_operator:
            self.downstream_operator.process(records)
        else:
            self.output_gate.push_all(records)
        return True

class Scheduler:
    """Scheduler is a single operator that flow control
       All the other operators. It cannot be chained"""

    def __init__(self, operator):
        self.downstream_operator = None
        self.input_gate = None
        self.output_gate = None
        self.event_time = operator.other_args[0]
        self.end_time = operator.other_args[1]
        self.time_step = operator.other_args[2]
        self.closed = False
        self.stateful_op = operator.stateful_op
        # todo: currently support one-stage stateful op
        self.num_input = len(self.stateful_op)
        self.marker = {}
        self.buffer_record = {}
        self.is_probe = False
        self.prober = operator.prober

    def set_chaining(self, downstream_operator, input_gate, output_gate):
        self.downstream_operator = downstream_operator
        self.output_gate = output_gate

    def process(self, record):
        if self.closed and self.is_probe:
            return False
        #if self.closed:
            #return False

        self.event_time = self.event_time + self.time_step
        if self.event_time > self.end_time:
            self.closed = True

        marker = {
            "event_type": "EventMarker",
            "event_time": self.event_time,
            "migration": None,
        }

        probe_msg = self.prober.probe()
        if probe_msg is not None:
            self.receive_probe(probe_msg, marker)
            self.is_probe = True

        #logger.info("marker send {}".format(marker))
        if self.closed is False:
            self.output_gate.broadcast_marker(marker)
            logger.info("marker send {}".format(marker))
        return True

    def receive_probe(self, record, marker):
        # field: sender, event_time
        logger.info("scheduler recv probe {}".format(record))

        event_time = record["event_time"]
        if self.marker.get(event_time) is None:
            self.marker[event_time] = 1
            self.buffer_record[event_time] = []
        else:
            self.marker[event_time] += 1
        self.buffer_record[event_time].append(record)
        if self.marker[event_time] == self.num_input:
            self.process_probe(event_time, marker)

    def process_probe(self, event_time, marker):
        # find max and min
        max_probe = max(self.buffer_record[event_time], key=lambda x: x["throughput"])
        min_probe = min(self.buffer_record[event_time], key=lambda x: x["throughput"])

        diff = max_probe["throughput"] - min_probe["throughput"]

        migrate_state = []
        if (diff / max_probe["throughput"]) > 0.99:
            # perform state migration
            while diff > 0:
                max_process = max(zip(max_probe["process"].values(),
                                      max_probe["process"].keys()))
                migrate_state.append(max_process[1])
                diff = diff - max_process[0]
                max_probe["process"].pop(max_process[1])

        if len(migrate_state) > 0:
            marker["migration"] = 1
            marker["sender"] = max_probe["sender"]
            marker["receiver"] = min_probe["sender"]
            marker["v_id"] = migrate_state

        self.buffer_record.pop(event_time)
        self.marker.pop(event_time)


class EventKeyBy:
    """A key_by operator instance that physically partitions the
    stream based on a key.
    """
    def __init__(self, operator):
        # Set the key selector
        self.key_selector = operator.other_args
        if isinstance(self.key_selector, int):
            self._key_selector = lambda r: r[self.key_selector]
        elif isinstance(self.key_selector, str):
            # self._key_selector = lambda record: vars(record)[self.key_selector]
            self._key_selector = lambda record: record[self.key_selector]
        elif not isinstance(self.key_selector, types.FunctionType):
            sys.exit("Unrecognized or unsupported key selector.")
        self.downstream_operator = None
        self.input_gate = None
        self.output_gate = None
        self.marker = {}
        self.num_input = operator.num_input
        self.partial_state = []
        self.buffer_record = {}

    def set_chaining(self, downstream_operator, input_gate, output_gate):
        self.downstream_operator = downstream_operator
        self.input_gate = input_gate
        self.output_gate = output_gate

    def process(self, record):
        if self.input_gate:
            record = self.input_gate.pull()
        if record is None:
            if self.downstream_operator:
                return self.downstream_operator.process(record)
            else:
                return False

        logger.info("record {}".format(record))
        if record["event_type"] == "EventMarker":
            event_time = record["event_time"]
            if self.marker.get(event_time) is None:
                self.marker[event_time] = 1
                if record["migration"] is not None:
                    v_ids = record["v_id"]
                    for v_id in v_ids:
                        self.partial_state.append(v_id)
                        self.buffer_record[v_id] = []
            else:
                self.marker[event_time] += 1
            if self.marker[event_time] == self.num_input:
                if record["migration"] is not None:
                    # change the routing table
                    # and flush buffer record
                    v_ids = record["v_id"]
                    for v_id in v_ids:
                        self.output_gate.hash.set(
                            v_id, record["receiver"])
                        self.flush(v_id, record["receiver"])
                self.output_gate.broadcast_marker(record)
                self.marker.pop(event_time)
            return True
        else:
            key = self._key_selector(record)
            v_id, actor_id = self.output_gate.hash.get(key)
            if v_id in self.partial_state:
                self.buffer_record[v_id].append(record)
                return True

            record["v_id"] = v_id

            if self.downstream_operator:
                self.downstream_operator.process((actor_id, record))
            else:
                self.output_gate.push((actor_id, record))
            return True

    def flush(self, v_id, actor_id):
        for record in self.buffer_record[v_id]:
            record["v_id"] = v_id
            self.output_gate.push((actor_id, record))
        self.buffer_record.pop(v_id)
        self.partial_state.remove(v_id)


class EventReduce:
    """A reduce operator instance that combines a new value for a key
    with the last reduced one according to a user-defined logic.
    """

    def __init__(self, operator):
        self.reduce_fn = operator.logic
        self.state = {}  # key -> value
        self.downstream_operator = None
        self.input_gate = None
        self.output_gate = None
        # for state migration
        self.marker = {}
        self.migrater = operator.migrater
        self.actor_id = operator.actor_id
        self.partial_state = []
        self.buffer_record = {}
        # for probe
        self.probe_writer = operator.probe_writer
        self.process_num = defaultdict(int)
        self.throughput = 0

    def set_chaining(self, downstream_operator, input_gate, output_gate):
        self.downstream_operator = downstream_operator
        self.input_gate = input_gate
        self.output_gate = output_gate

    def process(self, record):
        if self.input_gate:
            record = self.input_gate.pull()
        if record is None:
            if self.downstream_operator:
                return self.downstream_operator.process(record)
            else:
                return False

        if record["event_type"] == "EventMarker":
            # todo: stateless->keyby->reduce->keyby->stateful
            event_time = record["event_time"]
            if self.marker.get(event_time) is None:
                self.marker[event_time] = 1
                if (record["migration"] is not None) \
                        and (record["receiver"] == self.actor_id):
                    v_ids = record["v_id"]
                    for v_id in v_ids:
                        self.partial_state.append(v_id)
                        self.buffer_record[v_id] = []
            else:
                self.marker[event_time] += 1
            if self.marker[event_time] == self.input_gate.num_input:
                if record["migration"] is not None:
                    if record["sender"] == self.actor_id:
                        v_ids = record["v_id"]
                        for v_id in v_ids:
                            data = {
                                "event_type": "Migration",
                                "sender": self.actor_id,
                                "v_id": v_id,
                                "data": self.state[v_id]
                            }
                            self.state.pop(v_id)
                            logger.info("actor {} send migration data {}".format(self.actor_id, data))
                            self.migrater.write_migration(record["receiver"], data)
                    elif record["receiver"] == self.actor_id:
                        self.input_gate.start_migration()

                self.write_probe(event_time)
                self.marker.pop(event_time)
                # self.output_gate.broadcast(record)
            return True
        elif record["event_type"] == "Migration":
            logger.info("actor {} receive migration data {}".
                        format(self.actor_id, record))
            self.state[record["v_id"]] = record["data"]
            self.merge(record["v_id"])
            self.input_gate.end_migration()
            return True
        else:
            v_id = record["v_id"]
            if self.state.get(v_id) is None:
                if v_id in self.partial_state:
                    self.buffer_record[v_id].append(record)
                    return True
                else:
                    self.state[v_id] = {}

            result = self.reduce_fn(self.state[v_id], record)
            self.process_num[v_id] = self.process_num[v_id] + 1
            self.throughput = self.throughput + 1
            if self.downstream_operator:
                self.downstream_operator.process(result)
            else:
                self.output_gate.push(result)
            return True

    # Merge migrated state and record
    def merge(self, v_id):
        for record in self.buffer_record[v_id]:
            result = self.reduce_fn(self.state[v_id], record)
            self.process_num[v_id] = self.process_num[v_id] + 1
            self.throughput = self.throughput + 1

            if self.downstream_operator:
                self.downstream_operator.process(result)
            else:
                self.output_gate.push(result)

        self.buffer_record.pop(v_id)
        self.partial_state.remove(v_id)

    def write_probe(self, event_time):
        record = {
            "sender": self.actor_id,
            "event_time": event_time,
            "throughput": self.throughput,
            "process": self.process_num,
        }
        self.probe_writer.write_probe(record)
        self.process_num.clear()
        self.throughput = 0

    # Returns the state of the actor
    def get_state(self):
        return self.state


# A special source actor cooperated with Scheduler
class EventSource:
    def __init__(self, operator):
        # The user-defined source with a get_next() method
        self.source = operator.logic
        self.instance_id = operator.instance_id
        self.downstream_operator = None
        self.input_gate = None
        self.output_gate = None
        self.source.init(self.instance_id)
        self.marker = None

    def set_chaining(self, downstream_operator, input_gate, output_gate):
        self.downstream_operator = downstream_operator
        self.input_gate = input_gate
        self.output_gate = output_gate

    def process(self, record):
        if self.marker is None:
            self.marker = self.input_gate.pull()
            if self.marker is None:
                return False
        else:
            if self.source.can_get_next(self.marker["event_time"]):
                record = self.source.get_next()
                # if not record:
                #    return False
                if self.downstream_operator:
                    return self.downstream_operator.process(record)
                else:
                    self.output_gate.push(record)
            else:
                self.output_gate.broadcast_marker(self.marker)
                self.marker = None
        return True

# A custom source actor
class Source:
    def __init__(self, operator):
        # The user-defined source with a get_next() method
        self.source = operator.logic
        self.instance_id = operator.instance_id
        self.downstream_operator = None
        self.input_gate = None
        self.output_gate = None
        self.source.init(self.instance_id)

    # Starts the source by calling get_next() repeatedly
    def run(self, input_gate, output_gate):
        start = time.time()
        elements = 0
        while True:
            record = self.source.get_next()
            if not record:
                logger.debug("[writer] puts per second: {}".format(
                    elements / (time.time() - start)))
                return
            output_gate.push(record)
            elements += 1

    def set_chaining(self, downstream_operator, input_gate, output_gate):
        self.downstream_operator = downstream_operator
        self.output_gate = output_gate

    def process(self, record):
        record = self.source.get_next()
        if not record:
            return False
        if self.downstream_operator:
            return self.downstream_operator.process(record)
        else:
            self.output_gate.push(record)
        return True
