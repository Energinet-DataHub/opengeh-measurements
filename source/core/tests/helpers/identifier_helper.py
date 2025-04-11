import random
import string


def create_random_metering_point_id(position=8, digit=9):
    id = "".join(random.choice("0123456789") for _ in range(18))
    return id[:position] + str(digit) + id[position + 1 :]


def generate_random_string() -> str:
    return "".join(random.choices(string.ascii_letters, k=18))


def generate_random_binary() -> bytes:
    return bytes(random.getrandbits(8) for _ in range(18))
