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

# Used to join auctions with persons incrementally. An auction has exactly one
# seller, thus, we can remove the auction entry from local state upon a join
class JoinLogic(object):
    def __init__(self):
        # Local state
        self.auctions = {}  # seller -> auctions
        self.persons = {}   # id -> person

    def process_left(self, auction):
        result = []
        person = self.persons.get(auction["seller"])
        if person is None:  # Store auction for future join
            entry = self.auctions.setdefault(auction["seller"], [])
            entry.append(auction)
        else:  # Found a join; emit and do not store auction
            # logger.info("Found a join {} - {}".format(auction, person))
            p_time = person["system_time"]
            a_time = auction["system_time"]

            if p_time is None:
                s_time = a_time
            else:
                if a_time is not None and a_time > p_time:
                    s_time = a_time
                else:
                    s_time = p_time
            record = {}
            record["name"] = person["name"]
            record["city"] = person["city"]
            record["state"] = person["state"]
            record["person_id"] = person["id"]
            record["auction_id"] = auction["id"]
            
            result.append(record)
        return result

    def process_right(self, person):
        result = []
        self.persons.setdefault(person["id"],person)
        auctions = self.auctions.pop(person["id"], None)
        if auctions is not None:
            for auction in auctions:
                # logger.info("Found a join {} - {}".format(auction, person))
                # Remove entry
                p_time = person["system_time"]
                a_time = auction["system_time"]

                if p_time is None:
                    s_time = a_time
                else:
                    if a_time is not None and a_time > p_time:
                        s_time = a_time
                    else:
                        s_time = p_time

                record = {}
                record["name"] = person["name"]
                record["city"] = person["city"]
                record["state"] = person["state"]
                record["person_id"] = person["id"]
                record["auction_id"] = auction["id"]
                
                result.append(record)
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
    output = auctions_stream.join(persons_stream, JoinLogic())
    output = output.sind(dg.LatencySink())

    # stream to stdout
    start = time.time()
    env_handle = env.execute()  # Deploys and executes the dataflow
    ray.get(env_handle)  # Stay alive until execution finishes
    env.wait_finish()
    end = time.time()
    logger.info("Elapsed time: {} secs".format(end - start))
    logger.debug("Output stream id: {}".format(output.id))
