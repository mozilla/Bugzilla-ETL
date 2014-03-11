import types
import math
from ..collections.multiset import Multiset
from ..struct import Null

__author__ = 'klahnakoski'



def reverse(values):
    """
    REVERSE - WITH NO SIDE EFFECTS!
    """
    output = list(values)
    output.reverse()
    return output


def MIN(*values):
    if isinstance(values, tuple) and len(values) == 1 and isinstance(values[0], (list, set, tuple, Multiset, types.GeneratorType)):
        values = values[0]
    output = Null
    for v in values:
        if v == None:
            continue
        if isinstance(v, float) and math.isnan(v):
            continue
        if output == None:
            output = v
            continue
        output = min(output, v)
    return output


def MAX(*values):
    if isinstance(values, tuple) and len(values) == 1 and isinstance(values[0], (list, set, tuple, Multiset, types.GeneratorType)):
        values = values[0]
    output = Null
    for v in values:
        if v == None:
            continue
        if isinstance(v, float) and math.isnan(v):
            continue
        if output == None:
            output = v
            continue
        output = max(output, v)
    return output


def PRODUCT(*values):
    if isinstance(values, tuple) and len(values) == 1 and isinstance(values[0], (list, set, tuple, Multiset, types.GeneratorType)):
        values = values[0]
    output = Null
    for v in values:
        if v == None:
            continue
        if isinstance(v, float) and math.isnan(v):
            continue
        if output == None:
            output = v
            continue
        output *= v
    return output

def SUM(*values):
    if isinstance(values, tuple) and len(values) == 1 and isinstance(values[0], (list, set, tuple, Multiset, types.GeneratorType)):
        values = values[0]
    output = Null
    for v in values:
        if v == None:
            continue
        if isinstance(v, float) and math.isnan(v):
            continue
        if output == None:
            output = v
            continue
        output += v
    return output


def COUNT(*values):
    if isinstance(values, tuple) and len(values) == 1 and isinstance(values[0], (list, set, tuple, Multiset, types.GeneratorType)):
        values = values[0]
    output = 0
    for v in values:
        if v != None:
            output += 1
    return output


def SUM(*values):
    if isinstance(values, tuple) and len(values) == 1 and isinstance(values[0], (list, set, tuple, Multiset, types.GeneratorType)):
        values = values[0]
    output = Null
    for v in values:
        if v == None:
            continue
        if isinstance(v, float) and math.isnan(v):
            continue
        if output == None:
            output = v
            continue
        output += v
    return output


def AND(*values):
    if isinstance(values, tuple) and len(values) == 1 and isinstance(values[0], (list, set, tuple, Multiset, types.GeneratorType)):
        values = values[0]
    for v in values:
        if v == None:
            continue
        if not v:
            return False
    return True


def OR(*values):
    if isinstance(values, tuple) and len(values) == 1 and isinstance(values[0], (list, set, tuple, Multiset, types.GeneratorType)):
        values = values[0]
    for v in values:
        if v == None:
            continue
        if v:
            return True
    return False


def UNION(*values):
    if isinstance(values, tuple) and len(values) == 1 and isinstance(values[0], (list, set, tuple, Multiset, types.GeneratorType)):
        values = values[0]

    output = set()
    for v in values:
        if values == None:
            continue
        if isinstance(v, (list, set)):
            output.update(v)
            continue
        else:
            output.add(v)
    return output
