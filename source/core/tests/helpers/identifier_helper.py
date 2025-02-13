import random
import string


def create_random_metering_point_id():
    return "".join(random.choice("0123456789") for _ in range(18))


def generate_random_string() -> str:
    return "".join(random.choices(string.ascii_letters, k=18))
