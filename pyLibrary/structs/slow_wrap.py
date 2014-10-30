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
from types import GeneratorType
from ..struct import StructList, Struct


_get = object.__getattribute__
_set = object.__setattr__


def slow_wrap(v):
    return wrapper.get(_get(v, "__class__"), _no_wrap)(v)


def _wrap_dict(v):
    m = Struct()
    _set(m, "__dict__", v)  # INJECT m.__dict__=v SO THERE IS NO COPY
    return m


def _wrap_list(v):
    return StructList(v)


def _wrap_generator(v):
    return (slow_wrap(vv) for vv in v)


def _no_wrap(v):
    return v


wrapper = {
    dict: _wrap_dict,
    list: _wrap_list,
    GeneratorType: _wrap_generator
}

