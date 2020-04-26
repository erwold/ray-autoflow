import argparse
import logging
import time
from collections import defaultdict

import ray
from ray.streaming.config import Config
from ray.streaming.streaming import Environment, Conf
import data_generator as dg

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

parser = argparse.ArgumentParser()
parser.add_argument("--bids-file", required=True,
                    help="Path to the bids file")


# Used to count number of bids per auction
class AggregationLogic(object):
    def __init__(self):
        pass

    # Initializes bid counter
    def initialize(self, bid):
        return (bid["auction"], 1)

    def initialize_window(self):
        return defaultdict(int)

    # Updates number of bids per auction with the given record
    def update(self, old_state, bid):
        old_state[bid['auction']] += 1


if __name__ == "__main__":
    # Get program parameters
    args = parser.parse_args()
    bids_file = str(args.bids_file)

    ray.init(local_mode=False)

    # A Ray streaming environment with the default configuration
    env = Environment(config=Conf(channel_type=Config.NATIVE_CHANNEL))
    env.set_parallelism(1)  # Each operator will be executed by two actors

    # The following dataflow is a simple streaming wordcount
    #  with a rolling sum operator.
    # It reads articles from wikipedia, splits them in words,
    # shuffles words, and counts the occurences of each word.
    stream = env.source(dg.NexmarkEventGenerator(bids_file, "Bid")) \
                .key_by("auction") \
                .event_time_window(5000, 1000, aggregation_logic=AggregationLogic()) \
                .sink(dg.LatencySink())

    # stream to stdout
    start = time.time()
    env_handle = env.execute()  # Deploys and executes the dataflow
    ray.get(env_handle)  # Stay alive until execution finishes
    env.wait_finish()
    end = time.time()
    logger.info("Elapsed time: {} secs".format(end - start))
    logger.debug("Output stream id: {}".format(stream.id))
