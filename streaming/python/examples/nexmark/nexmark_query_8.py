import argparse
import logging
import time

import ray
from ray.streaming.config import Config
from ray.streaming.streaming import Environment, Conf
import data_generator as dg

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

parser = argparse.ArgumentParser()
parser.add_argument("--auctions-file", required=True,
                    help="Path to the auctions file")
parser.add_argument("--persons-file", required=True,
                    help="Path to the persons file")


# Used to join auctions with persons on a window. An auction has exactly one
# seller, thus, we can remove the auction entry from local state upon a join
class JoinLogic(object):
    def __init__(self):
        # Local state
        # self.auctions = {}  # window -> seller -> auctions
        # self.persons = {}   # window -> id -> person

        # self.auctions = defaultdict(lambda: defaultdict(list))
        # self.persons = defaultdict(lambda: defaultdict(lambda: None))
        self.left_state = [] # auctions
        self.right_state = [] # persons
        self.max_window = 0

    def process_left(self, window_range, auction):
        result = []
        for person in self.right_state:
            if person["dateTime"] > window_range[1]:
                break

            if person["id"] == auction["seller"]:
                record = {
                    "name": person["name"],
                    "city": person["city"],
                    "state": person["state"],
                    "person_id": person["id"],
                    "auction_id": auction["id"],
                }
                result.append(record)

        if len(result) == 0:
            self.left_state.append(auction)

        return result

    def process_right(self, window_range, person):
        result = []
        for auction in self.left_state:
            if auction["dateTime"] > window_range[1]:
                break

            if auction["seller"] == person["id"]:
                record = {
                    "name": person["name"],
                    "city": person["city"],
                    "state": person["state"],
                    "person_id": person["id"],
                    "auction_id": auction["id"],
                }
                result.append(record)

        self.right_state.append(person)

        return result

if __name__ == "__main__":
    # Get program parameters
    args = parser.parse_args()
    persons_file = str(args.persons_file)
    auctions_file = str(args.auctions_file)

    ray.init(local_mode=False)

    # A Ray streaming environment with the default configuration
    env = Environment(config=Conf(channel_type=Config.NATIVE_CHANNEL))
    env.set_parallelism(1)  # Each operator will be executed by two actors

    # The following dataflow is a streaming join
    # from two input stream.
    # one stream reads from auctions file, and the other reads
    # from persons file, both is partitioned by the person id field
    auctions_stream = env.source(dg.NexmarkEventGenerator(auctions_file, "Auction")) \
                         .key_by("seller")

    persons_stream = env.source(dg.NexmarkEventGenerator(persons_file, "Person")) \
                        .key_by("id")
    output = auctions_stream.time_window_join(persons_stream, 5000, 1000, JoinLogic())
    output = output.sink(dg.LatencySink())

    # stream to stdout
    start = time.time()
    env_handle = env.execute()  # Deploys and executes the dataflow
    ray.get(env_handle)  # Stay alive until execution finishes
    env.wait_finish()
    end = time.time()
    logger.info("Elapsed time: {} secs".format(end - start))
    logger.debug("Output stream id: {}".format(output.id))
