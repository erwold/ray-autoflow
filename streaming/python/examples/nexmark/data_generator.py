from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import time

from event import Auction
from event import Bid
from event import Person
from event import Record
from event import Watermark

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# A stream replayer that reads Nexmark events from files and
# replays them at given rates
class NexmarkEventGenerator(object):
    def __init__(self, event_file, event_type, event_rate=-1,
                       sample_period=1000, max_records=-1,
                       omit_extra=False):
        self.event_file = event_file
        self.max_records = max_records if max_records > 0 else float("inf")
        self.event_rate = event_rate  if event_rate > 0 else float("inf")
        self.event_type = event_type  # Auction, Bid, Person
        assert event_type in ["Auction","Bid","Person"]
        self.events = []
        self.omit_extra_field = omit_extra
        # Used for event replaying
        self.total_count = 0
        self.count = 0
        self.period = sample_period
        self.start = 0

        self.done = False

    # Parses a nexmark event log and creates an event object
    def __create_event(self, event, omit_extra_field=False):
        obj = Bid() if self.event_type == "Bid" else Person(
                            ) if self.event_type == "Person" else Auction()
        event = event.strip()[1:-1]  # Trim spaces and brackets
        raw_attributes = event.split(", ")
        attribute_value = []
        for attribute in raw_attributes:
            k_v = attribute.split(": ")
            key = k_v[0][1:-1]
            value = int(k_v[1]) if k_v[1].isdigit() else str(k_v[1])
            if (key != "extra") or (not omit_extra_field):
                setattr(obj, key, value)
        return obj.__dict__

    # Used to rate limit the source
    def __wait(self):
        while (self.total_count / (time.time() - self.start) >
               self.event_rate):
           time.sleep(0.00005)  # 50 us

    # Loads input file
    def init(self, instance_id):
        # Read all events from the input file
        logger.info("Loading input file...")
        records = 0
        self.event_file = self.event_file.format(instance_id)
        with open(self.event_file, "r") as ef:
            for event in ef:
                self.events.append(self.__create_event(event,
                                                  self.omit_extra_field))
                records += 1
                if records == self.max_records:
                    break
        # while len(self.events) < self.max_records:
        #    last_event_time = self.events[-1]['dateTime']
        #    events = []
        #    for event in self.events:
        #        event = dict(event)
        #        event['dateTime'] += last_event_time
        #        events.append(event)
        #    self.events += events
        logger.info("Done.")

    # Returns the next event
    def get_next_batch(self, batch_size):
        if not self.start:
            self.start = time.time()
        if self.total_count == len(self.events):
            self.done = True
        if self.done or (self.total_count >= self.max_records):
            return None  # Exhausted
        limit = min(len(self.events), self.total_count + batch_size)
        event_batch = self.events[self.total_count:limit]
        added_records = limit - self.total_count
        self.total_count += added_records
        self.__wait()  # Wait if needed
        self.count += added_records
        if self.count >= self.period:
            self.count = 0
            # Assign the generation timestamp
            # to the 1st record of the batch
            event_batch[0]["system_time"] = time.time()
        return event_batch

    # Returns the next event
    def get_next(self):
        if not self.start:
            self.start = time.time()
        if (not self.events) or (self.total_count == self.max_records):
            return None  # Exhausted
        event = self.events.pop(0)
        self.total_count += 1
        #self.__wait()  # Wait if needed
        self.count += 1
        if self.count == self.period:
            self.count = 0
            # Assign the generation timestamp
            event["system_time"] = time.time()
        else:
            event["system_time"] = None
        return event

    def can_get_next(self, event_time):
        if len(self.events) > 0:
            if self.events[0]["dateTime"] <= event_time:
                return True
        return False

    def get_batch_before(self, event_time):
        batch = []
        if (not self.events) or (self.total_count == self.max_records):
            return None  # Exhausted
        while self.events[0]["dateTime"] <= event_time:
            event = self.events.pop(0)
            self.total_count += 1
            self.count += 1
            if self.count == self.period:
                self.count = 0
                # Assign the generation timestamp
                event["system_time"] = time.time()
            batch.append(event)
        return batch

    # Drains the source as fast as possible
    def drain(self):
        self.event_rate = float("inf")  # Set rate limit to inf
        records = 0
        while self.get_next() is not None:
            records += 1
        return records

# A custom sink used to measure processing latency
class LatencySink(object):
    def __init__(self):
        self.latencies = []
        self.throughputs = []
        self.last_time = None
        self.num_records = 0
        self.is_first = False

    # Evicts next record
    def evict(self, record):
        # if record["event_type"] == "Watermark":
        #    return  # Ignore watermarks
        if self.is_first is False:
            self.is_first = True
            self.last_time = time.time()

        self.num_records = self.num_records + 1
        if self.num_records == 1000:
            now_time = time.time()
            throughput = self.num_records / (now_time - self.last_time)
            self.throughputs.append(throughput)
            self.last_time = now_time
            self.num_records = 0

        generation_time = record["system_time"]
        if generation_time is not None:
            # TODO (john): Clock skew might distort elapsed time
            latency = time.time() - generation_time
            self.latencies.append(latency)
            return latency
        return None

    # Closes the sink
    def close(self):
        pass

    # Returns sink's state
    def get_state(self):
        return self.state
