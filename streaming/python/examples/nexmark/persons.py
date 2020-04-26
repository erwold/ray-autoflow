import random

class Persons(object):
    def __init__(self):
        self.high_chunk = 1
        self.curr_id = 0
        self.person_distr_size = 100
        random.seed(283494)

    def get_new_id(self):
        new_id = self.curr_id
        self.curr_id = self.curr_id + 1
        if new_id == self.high_chunk * self.person_distr_size:
            self.high_chunk = self.high_chunk + 1
        return new_id

    def get_existing_id(self):
        id = random.randrange(self.person_distr_size)
        id += self.get_random_chunk_offset()
        return id % self.curr_id

    def get_random_chunk_offset(self):
        chunk_id = random.randrange(self.high_chunk)
        return chunk_id * self.person_distr_size
