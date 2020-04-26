import random

class OpenAuction():
    def __init__(self, cur_price, end_time):
        self.cur_price = cur_price
        self.end_time = end_time
        self.closed = False

    def increase_price(self, price):
        self.cur_price = self.cur_price + price
        return self.cur_price

    def get_cur_price(self):
        return self.cur_price

    def get_end_time(self):
        return self.end_time

    def check_closed(self, cur_time):
        #print("cur_time {} end_time {}".format(cur_time, self.end_time))
        if self.closed is False and cur_time > self.end_time:
            self.closed = True

    def is_closed(self, cur_time):
        self.check_closed(cur_time)
        return self.closed

