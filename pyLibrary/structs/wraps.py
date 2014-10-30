# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals
from __future__ import division
from types import NoneType, GeneratorType
from ..struct import Null, StructList, Struct


_get = object.__getattribute__
_set = object.__setattr__


def wrap(v):
    """
    THIS IS THE CANDIDATE WE ARE TESTING TO WRAP FASTER, BUT DOES NOT SEEM TO BE
    """
    type_ = _get(v, "__class__")

    if type_ is dict:
        m = Struct()
        _set(m, "__dict__", v)  # INJECT m.__dict__=v SO THERE IS NO COPY
        return m
    elif type_ is list:
        return StructList(v)
    elif type_ is GeneratorType:
        return (wrap(vv) for vv in v)
    elif type_ is NoneType:
        return Null
    else:
        return v


def wrap_dot(value):
    """
    dict WITH DOTS IN KEYS IS INTERPRETED AS A PATH
    """
    return wrap(_wrap_dot(value))


def _wrap_dot(value):
    if value == None:
        return None
    if isinstance(value, (basestring, int, float)):
        return value
    if isinstance(value, dict):
        if isinstance(value, Struct):
            value = unwrap(value)

        output = {}
        for key, value in value.iteritems():
            value = _wrap_dot(value)

            if key == "":
                from ..env.logs import Log

                Log.error("key is empty string.  Probably a bad idea")
            if isinstance(key, str):
                key = key.decode("utf8")

            d = output
            if key.find(".") == -1:
                if value is None:
                    d.pop(key, None)
                else:
                    d[key] = value
            else:
                seq = split_field(key)
                for k in seq[:-1]:
                    e = d.get(k, None)
                    if e is None:
                        d[k] = {}
                        e = d[k]
                    d = e
                if value == None:
                    d.pop(seq[-1], None)
                else:
                    d[seq[-1]] = value
        return output
    if hasattr(value, '__iter__'):
        output = []
        for v in value:
            v = wrap_dot(v)
            output.append(v)
        return output
    return value


def unwrap(v):
    _type = _get(v, "__class__")
    if _type is Struct:
        d = _get(v, "__dict__")
        return d
    elif _type is StructList:
        return v.list
    elif _type is NullType:
        return None
    elif _type is GeneratorType:
        return (unwrap(vv) for vv in v)
    else:
        return v


def listwrap(value):
    """
    OFTEN IT IS NICE TO ALLOW FUNCTION PARAMETERS TO BE ASSIGNED A VALUE,
    OR A list-OF-VALUES, OR NULL.  CHECKING FOR THIS IS TEDIOUS AND WE WANT TO CAST
    FROM THOSE THREE CASES TO THE SINGLE CASE OF A LIST

    Null -> []
    value -> [value]
    [...] -> [...]  (unchanged list)

    # BEFORE
    if a is not None:
        if not isinstance(a, list):
            a=[a]
        for x in a:
            # do something


    # AFTER
    for x in listwrap(a):
        # do something

    """
    if value == None:
        return []
    elif isinstance(value, list):
        return wrap(value)
    else:
        return wrap([unwrap(value)])


def tuplewrap(value):
    """
    INTENDED TO TURN lists INTO tuples FOR USE AS KEYS
    """
    if isinstance(value, (list, set, tuple, GeneratorType)):
        return tuple(tuplewrap(v) if isinstance(v, (list, tuple, GeneratorType)) else v for v in value)
    return unwrap(value),


from ..struct import StructList, Struct, split_field, NullType
