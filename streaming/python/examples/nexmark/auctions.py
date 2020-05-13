import random
import xxhash
from auction import OpenAuction
from constant import min_auction_time, max_auction_time

item_dist_size = 100
slots_meta = [(0, 38005339),
         (266914649, 392418729),
         (828625487, 895577465),
         (895577465, 1030162152),
         (2052894126, 2145941112),
         (3397088106, 3410051515),
         (3704878703, 3979594992),
         (4014175280, 4034431209),
         (4034431209, 4091524477),
         (4227403771, 4284104139)]

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

    def get_new_id(self, cur_time):
        id = self.cur_id
        end_time = cur_time + random.randrange(max_auction_time) + min_auction_time
        print("generate id {} end_time {}".format(id, end_time))
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

        is_skew = random.randrange(3)
        if is_skew == 1:
            ids = random.sample(self.skew_auctions.keys(), 1)
        else:
            ids = random.sample(self.auctions.keys(), 1)
        return ids[0]

    def try_shrink(self, cur_time):
        close_ids = []
        for id in self.auctions.keys():
            if self.auctions[id].is_closed(cur_time) is True:
                close_ids.append(id)
                print("cur_time {} closed_id: {}".format(cur_time, id))

        for id in close_ids:
            self.auctions.pop(id)

        close_ids = []
        for id in self.skew_auctions.keys():
            if self.skew_auctions[id].is_closed(cur_time) is True:
                close_ids.append(id)
                print("cur_time {} closed_id: {}".format(cur_time, id))

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
