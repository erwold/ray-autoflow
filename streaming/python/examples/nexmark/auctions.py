import random
from auction import OpenAuction
from constant import min_auction_time, max_auction_time

item_dist_size = 100

class OpenAuctions(object):
    def __init__(self):
        self.low_chunk = 0
        self.high_chunk = 1
        self.cur_id = 0
        self.closed_id = 0
        self.auctions = {}
        random.seed(18394)

    def get_new_id(self, cur_time):
        id = self.cur_id
        end_time = cur_time + random.randrange(max_auction_time) + min_auction_time
        print("generate id {} end_time {}".format(id, end_time))
        # self.auctions.append(OpenAuction(random.randrange(200)+1, end_time))
        self.auctions[id] = OpenAuction(random.randrange(200)+1, end_time)
        self.cur_id = self.cur_id + 1
        # if self.cur_id == self.high_chunk * item_dist_size:
        #    self.high_chunk = self.high_chunk + 1
        return id

    def get_existing_id(self, cur_time):
        # first check if there is closed auction
        self.try_shrink(cur_time)

        ids = random.sample(self.auctions.keys(), 1)
        return ids[0]
        #while True:
        #    id = random.randrange(item_dist_size)
        #    id = id + self.get_random_chunk_offset()

        # print("get id: {} cur_id: {} cur_time: {}".format(id-self.closed_id, self.cur_id, cur_time))
        #    if id < self.cur_id:
        #        if self.auctions[id-self.closed_id].is_closed(cur_time) is not True:
        #            return id + self.closed_id

    def try_shrink(self, cur_time):
        #if cur_time < 3000:
        #    print("cur_time {} end_time {}".format(cur_time, self.auctions[0].end_time))
        close_ids = []
        for id in self.auctions.keys():
            if self.auctions[id].is_closed(cur_time) is True:
                close_ids.append(id)
                print("cur_time {} closed_id: {}".format(cur_time, id))

        for id in close_ids:
            self.auctions.pop(id)

        # while self.auctions[0].is_closed(cur_time):
        #    self.auctions.remove(self.auctions[0])
        #    self.closed_id = self.closed_id + 1
        #    print("closed_id: {}".format(self.closed_id))


    # def get_random_chunk_offset(self):
    #    chunk_id = random.randrange(self.high_chunk - self.low_chunk) + self.low_chunk
    #    return chunk_id * item_dist_size - self.closed_id

    def increase_price(self, id):
        return self.auctions[id].increase_price(random.randrange(25) + 1)

    def get_end_time(self, id):
        return self.auctions[id].get_end_time()

    def get_cur_price(self, id):
        return self.auctions[id].get_cur_price()
