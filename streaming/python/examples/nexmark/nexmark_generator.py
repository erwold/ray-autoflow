import random
import string
import json
import persons as ps
import person_generator as pg
import timer as t
from auctions import OpenAuctions
from event import Auction, Bid

def generate_random_str(length):
    assert length > 0
    str_list = [random.choice(string.ascii_letters) for _ in range(length)]
    random_str = "".join(str_list)
    return random_str

class NexmarkGenerator(object):
    def __init__(self, calls, bids_files, persons_files, auctions_files):
        self.calls = calls
        self.bids_files = []
        self.persons_files = []
        self.auctions_files = []
        self.bids_index = 0
        self.persons_index = 0
        self.auctions_index = 0
        for i in range(len(bids_files)):
            self.bids_files.append(open(bids_files[i], "w"))
            self.persons_files.append(open(persons_files[i], "w"))
            self.auctions_files.append(open(auctions_files[i], "w"))

        self.timer = t.timer()
        self.persons = ps.Persons()
        self.person_generator = pg.generate_person()
        self.auctions = OpenAuctions()
        random.seed(103984)

    def generate_stream(self):
        for _ in range(50):
            self.generate_person()

        for _ in range(50):
            self.generate_auctions(1)

        for _ in range(self.calls):
            if random.randrange(10) == 0:
                self.generate_person()

            num_items = random.randrange(3)
            self.generate_auctions(num_items)

            num_bids = random.randrange(21)
            self.generate_bids(num_bids)

    def generate_person(self):
        self.timer.increment_time()
        person = self.person_generator.generate_person(self.persons.get_new_id(), self.timer.get_time())
        json.dump(person.__dict__, self.persons_files[self.persons_index])
        self.persons_files[self.persons_index].write("\n")
        self.persons_index = self.persons_index + 1
        if self.persons_index == len(self.persons_files):
            self.persons_index = 0

    def generate_auctions(self, num_items):
        self.timer.increment_time()

        for i in range(num_items):
            auction_id = self.auctions.get_new_id(self.timer.get_time())
            item_name = generate_random_str(random.randint(4, 15))
            description = generate_random_str(random.randint(30, 80))
            initial_bid = self.auctions.get_cur_price(auction_id)
            reserve = round(self.auctions.get_cur_price(auction_id)*(1.2+random.random()+1))
            date_time = self.timer.get_time()
            expires = self.auctions.get_end_time(auction_id)
            seller = self.persons.get_existing_id()
            category = random.randrange(303)
            extra = generate_random_str(random.randint(128, 300))

            auction = Auction(auction_id, item_name, description, initial_bid, reserve,
                              date_time, expires, seller, category, extra)

            json.dump(auction.__dict__, self.auctions_files[self.auctions_index])
            self.auctions_files[self.auctions_index].write("\n")
            self.auctions_index = self.auctions_index + 1
            if self.auctions_index == len(self.auctions_files):
                self.auctions_index = 0

    def generate_bids(self, num_bids):
        self.timer.increment_time()
        cur_time = self.timer.get_time()

        for i in range(num_bids):
            item_id = self.auctions.get_existing_id(cur_time)
            bidder = self.persons.get_existing_id()
            price = self.auctions.increase_price(item_id)
            extra = generate_random_str(random.randint(45, 85))

            bid = Bid(item_id, bidder, price, cur_time, extra)
            json.dump(bid.__dict__, self.bids_files[self.bids_index])
            self.bids_files[self.bids_index].write("\n")
            self.bids_index = self.bids_index + 1
            if self.bids_index == len(self.bids_files):
                self.bids_index = 0
