import random

max_incremet_secs = 60
class timer(object):
    def __init__(self):
        self.time_sec = 0
        random.seed(15423)

    def get_time(self):
        return self.time_sec

    def increment_time(self):
        self.time_sec = self.time_sec + random.randrange(max_incremet_secs)
