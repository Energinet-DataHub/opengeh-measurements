import random


def create_random_metering_point_id():
    return "".join(random.choice("0123456789") for _ in range(18))
