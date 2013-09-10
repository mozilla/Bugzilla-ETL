import random
import string


SIMPLE_ALPHABET=string.ascii_letters + string.digits
SEED=random.Random()


class Random():

    @staticmethod
    def string(length, alphabet=SIMPLE_ALPHABET):
        result = ''
        for i in range(0, length):
            result += SEED.choice(alphabet)
        return result

    @staticmethod
    def hex(length):
        return Random.string(length, string.digits + 'ABCDEF')

    @staticmethod
    def int(*args):
        return random.randrange(*args)
