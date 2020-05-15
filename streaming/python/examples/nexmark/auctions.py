import random
import xxhash
from auction import OpenAuction
from constant import min_auction_time, max_auction_time

num_node = 2
num_slot = 20
skew_node = [0]
skew_percent = 5
slots_meta = []
"""slots_meta = [(0, 242428685),
         (562254147, 585553396),
         (1709671711, 1735916491),
         (1735916491, 2223001858),
         (2223001858, 2443748874),
         (2443748874, 2782254823),
         (2835507185, 2869437224),
         (3162023598, 3264356174),
         (3264356174, 3268739810),
         (3268739810, 4125618242)]"""

def generate_slots():
    sorted_slots = []  # [.....]
    slots = []  # [[...], [...], ...]
    for i in range(num_node):
        slot = []
        for j in range(num_slot):
            slot_hash = xxhash.xxh32_intdigest("{}-{}".format(i, j))
            slot.append(slot_hash)
            sorted_slots.append(slot_hash)
        slots.append(slot)

    sorted_slots = sorted(sorted_slots)
    skew_slots = []
    for node in skew_node:
        for slot in slots[node]:
            skew_slots.append(slot)

    skew_slots = sorted(skew_slots)
    for slot in skew_slots:
        index = sorted_slots.index(slot)
        if index == 0:
            slots_meta.append((0, sorted_slots[index]))
            # slots_meta.append((sorted_slots[-1], ))
        else:
            slots_meta.append((sorted_slots[index-1], sorted_slots[index]))

    print(slots_meta)

def is_in_slots(id):
    key = xxhash.xxh32_intdigest(str(id))
    for slot_meta in slots_meta:
        if (key >= slot_meta[0]) and (key < slot_meta[1]):
            return True
    return False

class OpenAuctions(object):
    def __init__(self):
        self.low_chunk = 0
        self.high_chunk = 1
        self.cur_id = 0
        self.closed_id = 0
        self.auctions = {}
        self.skew_auctions = {}
        random.seed(18394)
        generate_slots()

    def get_new_id(self, cur_time):
        id = self.cur_id
        end_time = cur_time + random.randrange(max_auction_time) + min_auction_time
        #print("generate id {} end_time {}".format(id, end_time))
        # self.auctions.append(OpenAuction(random.randrange(200)+1, end_time))
        if is_in_slots(id):
            self.skew_auctions[id] = OpenAuction(random.randrange(200)+1, end_time)
        else:
            self.auctions[id] = OpenAuction(random.randrange(200)+1, end_time)
        self.cur_id = self.cur_id + 1
        return id

    def get_existing_id(self, cur_time):
        # first check if there is closed auction
        self.try_shrink(cur_time)

        is_skew = random.randrange(10)+1
        if is_skew <= skew_percent:
            ids = random.sample(self.skew_auctions.keys(), 1)
        else:
            ids = random.sample(self.auctions.keys(), 1)
        return ids[0]

    def try_shrink(self, cur_time):
        close_ids = []
        for id in self.auctions.keys():
            if self.auctions[id].is_closed(cur_time) is True:
                close_ids.append(id)
                #print("cur_time {} closed_id: {}".format(cur_time, id))

        for id in close_ids:
            self.auctions.pop(id)

        close_ids = []
        for id in self.skew_auctions.keys():
            if self.skew_auctions[id].is_closed(cur_time) is True:
                close_ids.append(id)
                #print("cur_time {} closed_id: {}".format(cur_time, id))

        for id in close_ids:
            self.skew_auctions.pop(id)

    # def get_random_chunk_offset(self):
    #    chunk_id = random.randrange(self.high_chunk - self.low_chunk) + self.low_chunk
    #    return chunk_id * item_dist_size - self.closed_id

    def increase_price(self, id):
        if self.auctions.get(id) is not None:
            return self.auctions[id].increase_price(random.randrange(25) + 1)
        elif self.skew_auctions.get(id) is not None:
            return self.skew_auctions[id].increase_price(random.randrange(25) + 1)
        else:
            print("[increase_price] unknown id {}".format(id))
            exit()

    def get_end_time(self, id):
        if self.auctions.get(id) is not None:
            return self.auctions[id].get_end_time()
        elif self.skew_auctions.get(id) is not None:
            return self.skew_auctions[id].get_end_time()
        else:
            print("[get_end_time] unknown id {}".format(id))
            exit()

    def get_cur_price(self, id):
        if self.auctions.get(id) is not None:
            return self.auctions[id].get_cur_price()
        elif self.skew_auctions.get(id) is not None:
            return self.skew_auctions[id].get_cur_price()
        else:
            print("[get_cur_price] unknown id {}".format(id))
            exit()
