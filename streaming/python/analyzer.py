import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class Analyzer(object):
    def __init__(self, actors):
        self.actor_index = {}
        self.num_actor = 0
        self.window_length = 10
        self.slot_num = 10
        self.actors = actors
        for actor in actors:
            self.actor_index[actor] = self.num_actor
            # self.window_length[self.num_actor] = 10
            self.num_actor += 1
        logger.info("actor_index {}".format(self.actor_index))

        self.init = [False] * self.num_actor
        # actor_index->v_id->list
        self.slot_data = {}
        self.throughput_data = {}

        self.num_time = 0
        self.time_index = {}

        self.last_time_id = 0
        self.last_migrate_time = None

    def init_data(self, record):
        actor_id = record["sender"]
        id = self.actor_index[actor_id]

        self.slot_data[id] = {}
        self.init[id] = True

    def process_probe(self, record):
        event_time = record["event_time"]
        if self.time_index.get(event_time) is None:
            self.time_index[event_time] = self.num_time
            self.num_time += 1

        id = self.actor_index[record["sender"]]
        if self.init[id] is False:
            self.init_data(record)

        if self.throughput_data.get(id) is None:
            self.throughput_data[id] = []
        self.throughput_data[id].append(record["throughput"])

        slot_data = record["process"]
        for v_id in slot_data.keys():
            if self.slot_data[id].get(v_id) is None:
                self.slot_data[id][v_id] = []
            self.slot_data[id][v_id].append(slot_data[v_id])

        logger.info("th_data {}".format(self.throughput_data))
        logger.info("sl_data {}".format(self.slot_data))

    def analyze(self, event_time, marker):
        # time_id consider equal window end
        time_id = self.time_index[event_time]
        if self.last_migrate_time is not None:
            if event_time == self.last_migrate_time:
                self.last_migrate_time = None
            self.clear()
            self.last_time_id = time_id
            logger.info("event_time {} unreach last_migrate_time {}".
                        format(event_time, self.last_migrate_time))
            return

        time_step = time_id - self.last_time_id
        if time_step != self.window_length:
            return

        self.last_time_id = time_id

        sum_throughput = {}
        sum_slot = {}

        # start = time_id - self.window_length + 1
        # end = time_id + 1

        for actor in self.actor_index.keys():
            id = self.actor_index[actor]
            sum_throughput[id] = 0
            sum_slot[id] = {}

            for num in self.throughput_data[id]:
                sum_throughput[id] += num

            for v_id in self.slot_data[id].keys():
                sum_slot[id][v_id] = 0
                for num in self.slot_data[id][v_id]:
                    sum_slot[id][v_id] += num

        # do migration
        logger.info("sum_throughput {}".format(sum_throughput))
        logger.info("sum_slot {}".format(sum_slot))
        # tuple (throughput, actor_index)
        max_throughput = max(zip(sum_throughput.values(),
                                 sum_throughput.keys()))
        min_throughput = min(zip(sum_throughput.values(),
                                 sum_throughput.keys()))
        max_id = max_throughput[1]
        min_id = min_throughput[1]

        diff = max_throughput[0] - min_throughput[0]

        migrate_state = []
        if (diff / max_throughput[0]) >= 0.2:
            gap = diff / 2
            while gap > 0:
                max_probe = max(zip(sum_slot[max_id].values(),
                                    sum_slot[max_id].keys()))
                migrate_state.append(max_probe[1])
                sum_slot[max_id].pop(max_probe[1])
                gap = gap - max_probe[0]

        if len(migrate_state) > 0:
            marker["migration"] = 1
            marker["sender"] = self.actors[max_id]
            marker["receiver"] = self.actors[min_id]
            marker["v_id"] = migrate_state
            self.last_migrate_time = marker["event_time"]

        self.clear()

    def clear(self):
        self.throughput_data.clear()
        for id in self.actor_index.values():
            self.slot_data[id].clear()

