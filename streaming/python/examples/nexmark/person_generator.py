import random
import string
from event import Person
from firstname import firstnames, num_firstnames
from lastname import lastnames, num_lastnames
from constant import emails, num_emails
from constant import cities, num_cities
from constant import states, num_states
from constant import min_person_extra, max_person_extra

def generate_random_str(length):
    assert length > 0
    str_list = [random.choice(string.ascii_letters) for i in range(length)]
    random_str = "".join(str_list)
    return random_str

class generate_person(object):
    def __init__(self):
        random.seed(20934)

    def generate_person(self, id, cur_time):
        # genrate Name field
        firstname = firstnames[random.randrange(num_firstnames)]
        lastname = lastnames[random.randrange(num_lastnames)]
        name = firstname + " " + lastname

        # generate emailAddress field
        email = lastname + "@" + emails[random.randrange(num_emails)]

        # generate credit-card field
        credit_card = str(random.randrange(9000) + 1000)
        credit_card = credit_card + " " + str(random.randrange(9000) + 1000)
        credit_card = credit_card + " " + str(random.randrange(9000) + 1000)
        credit_card = credit_card + " " + str(random.randrange(9000) + 1000)

        # generate city, state, time
        city = cities[random.randrange(num_cities)]
        state = states[random.randrange(num_states)]
        date_time = cur_time

        # generate extra string
        num_digits = random.randint(min_person_extra, max_person_extra)
        extra = generate_random_str(num_digits)
        return Person(id=id, name=name, email=email, credit_card=credit_card,
                      city=city, state=state, date_time=date_time, extra=extra)
