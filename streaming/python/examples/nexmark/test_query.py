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

def map_function(bid):
    record = {
        "auction": bid["auction"],
        "price": bid["price"],
        "system_time": bid["system_time"],
        "event_time": bid["event_time"],
    }
    return record

def reduce_function(state, bid):
    if state.get(bid["auction"]) is None:
        state[bid["auction"]] = 0
    state[bid["auction"]] += bid["price"]
    record = {
        "auction": bid["auction"],
        "price": state[bid["auction"]],
        "system_time": bid["system_time"],
    }
    return record


if __name__ == "__main__":
    # Get program parameters
    args = parser.parse_args()
    bids_file = str(args.bids_file)

    #ray.init(address="auto", redis_password="5241590000000000")
    ray.init(local_mode=False)

    # A Ray streaming environment with the default configuration
    env = Environment(config=Conf(channel_type=Config.NATIVE_CHANNEL))
    #env.set_parallelism(1)  # Each operator will be executed by two actors

    # The following dataflow is a simple streaming wordcount
    #  with a rolling sum operator.
    # It reads articles from wikipedia, splits them in words,
    # shuffles words, and counts the occurences of each word.
    stream = env.scheduler(2800, 1800000, 1000) \
                .shuffle() \
                .set_parallelism(1) \
                .event_source(dg.NexmarkEventGenerator(bids_file, "Bid")) \
                .set_parallelism(3) \
                .map(map_function) \
                .set_parallelism(3) \
                .event_key_by("auction") \
                .set_parallelism(3) \
                .event_reduce(reduce_function) \
                .set_parallelism(3) \
                .sink(dg.LatencySink()) \
                .set_parallelism(3)

    # stream to stdout
    start = time.time()
    env_handle = env.execute()  # Deploys and executes the dataflow
    ray.get(env_handle)  # Stay alive until execution finishes
    env.wait_finish()
    end = time.time()
    logger.info("Elapsed time: {} secs".format(end - start))
    logger.debug("Output stream id: {}".format(stream.id))
