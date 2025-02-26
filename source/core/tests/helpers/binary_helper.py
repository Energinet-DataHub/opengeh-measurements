import random


def generate_random_binary() -> bytearray:
    return bytearray(random.getrandbits(8) for _ in range(18))
