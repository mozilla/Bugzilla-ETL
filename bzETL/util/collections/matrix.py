# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
from ..env.logs import Log
from ..math.maths import Math
from ..struct import Null


class Matrix(object):
    """
    SIMPLE n-DIMENSIONAL ARRAY OF OBJECTS
    """

    def __init__(self, *dims):
        self.num = len(dims)
        self.dims = dims
        self.cube = _null(*dims)

    def __getitem__(self, index):
        if isinstance(index, slice):
            pass

    def __setitem__(self, key, value):
        if len(key) != self.num:
            Log.error("Expecting coordinates to match the number of dimensions")
        last = self.num - 1
        m = self.cube
        for k in key[0:last:]:
            m = m[k]
        m[key[last]] = value

    def __len__(self):
        return Math.product(self.dims)

    def __iter__(self):
        return _iter(self.cube, self.num)




def _iter(cube, depth):
    if depth == 1:
        def iterator():
            for c in cube:
                yield c
        return iterator()
    else:
        def iterator():
            for c in cube:
                for b in _iter(c, depth-1):
                    yield b
        return iterator()


def _null(*dims):
    d0 = dims[0]
    if d0 == 0:
        Log.error("Zero dimensions not allowed")
    if len(dims) == 1:
        return [Null for i in range(dims[0])]
    else:
        return [_null(*dims[1::]) for i in range(dims[0])]
