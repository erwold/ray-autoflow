import logging
import sys
import time
import types

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
        logger.info("source push {}".format(record[:-1]))
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
        if record is None:
            record = self.input_gate.pull()
            if record is None:
                return False
        record = self.map_fn(record)

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
        if record is None:
            record = self.input_gate.pull()
            if record is None:
                return False
        logger.info("flatmap {}".format(record))
        records = self.flatmap_fn(record)

        logger.info("after flatmap {}".format(records))
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
        if record is None:
            record = self.input_gate.pull()
            if record is None:
                return False
        record = self.filter_fn(record)

        if self.downstream_operator:
            self.downstream_operator.process(record)
        else:
            self.output_gate.push(record)
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
        if record is None:
            record = self.input_gate.pull()
            if record is None:
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
        if record is None:
            record = self.input_gate.pull()
            if record is None:
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
            self._key_selector = lambda record: vars(record)[self.key_selector]
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
        if record is None:
            record = self.input_gate.pull()
            if record is None:
                return False
        logger.info("keyby record {}".format(record))
        key = self._key_selector(record)
        logger.info("after keyby record {}".format((key, record)))

        if self.downstream_operator:
            self.downstream_operator.process((key, record))
        else:
            self.output_gate.push((key, record))
        return True


# A custom source actor
class Source:
    def __init__(self, operator):
        # The user-defined source with a get_next() method
        self.source = operator.logic
        self.downstream_operator = None
        self.input_gate = None
        self.output_gate = None

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

    def set_chaining(self, downstream_operator, inputgate, output_gate):
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
