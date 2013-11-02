# encoding: utf-8
#
import random
import string


SIMPLE_ALPHABET=string.ascii_letters + string.digits
SEED=random.Random()


class Random(object):

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

    @staticmethod
    def sample(data, count):
        num=len(data)
        return [data[Random.int(num)] for i in range(count)]
