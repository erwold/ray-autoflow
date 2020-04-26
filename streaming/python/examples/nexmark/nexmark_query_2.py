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
parser.add_argument("--bids-file", required=True,
                    help="Path to the bids file")

# Used to filter auctions based on their ID. A filter could be used instead
def flatmap_function(bid):
    if (bid["auction"] == "1007" or bid["auction"] == "1020" or bid["auction"] == "2001" or
        bid["auction"] == 2019 or bid["auction"] == "2087") or (
        bid["system_time"] is not None):  # Use these bids to measure latency
        # record = Record(auction=bid.auction, price=bid.price,
        #                 system_time=bid.system_time)
        return [bid]
    return []

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
                .flat_map(flatmap_function) \
                .sink(dg.LatencySink())

    # stream to stdout
    start = time.time()
    env_handle = env.execute()  # Deploys and executes the dataflow
    ray.get(env_handle)  # Stay alive until execution finishes
    env.wait_finish()
    end = time.time()
    logger.info("Elapsed time: {} secs".format(end - start))
    logger.debug("Output stream id: {}".format(stream.id))
