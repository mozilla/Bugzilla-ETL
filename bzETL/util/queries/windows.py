# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from ..logs import Log
from ..maths import Math
from ..multiset import Multiset
from ..stats import Z_moment, stats2z_moment, z_moment2stats

class AggregationFunction(object):
    def __init__(self):
        """
        RETURN A ZERO-STATE AGGREGATE
        """
        Log.error("not implemented yet")

    def add(self, value):
        """
        ADD value TO AGGREGATE
        """
        Log.error("not implemented yet")


    def merge(self, agg):
        """
        ADD TWO AGGREGATES TOGETHER
        """
        Log.error("not implemented yet")

    def end(self):
        """
        RETURN AGGREGATE
        """


class WindowFunction(AggregationFunction):

    def __init__(self):
        """
        RETURN A ZERO-STATE AGGREGATE
        """
        Log.error("not implemented yet")


    def sub(self, value):
        """
        REMOVE value FROM AGGREGATE
        """
        Log.error("not implemented yet")




class Stats(WindowFunction):

    def __init__(self):
        object.__init__(self)
        self.total=Z_moment(0,0,0)


    def add(self, value):
        if value == None:
            return
        self.total+=stats2z_moment(value)

    def sub(self, value):
        if value == None:
            return
        self.total-=stats2z_moment(value)

    def merge(self, agg):
        self.total+=agg.total

    def end(self):
        return z_moment2stats(self.total)


class Min(WindowFunction):
    def __init__(self):
        object.__init__(self)
        self.total = Multiset()


    def add(self, value):
        if value == None:
            return
        self.total.add(value)

    def sub(self, value):
        if value == None:
            return
        self.total.remove(value)

    def end(self):
        return Math.min(self.total)


class Max(WindowFunction):
    def __init__(self):
        object.__init__(self)
        self.total = Multiset()


    def add(self, value):
        if value == None:
            return
        self.total.add(value)

    def sub(self, value):
        if value == None:
            return
        self.total.remove(value)

    def end(self):
        return Math.max(self.total)


class Count(WindowFunction):
    def __init__(self):
        object.__init__(self)
        self.total = 0


    def add(self, value):
        if value == None:
            return
        self.total += 1

    def sub(self, value):
        if value == None:
            return
        self.total -= 1

    def end(self):
        return self.total


class Sum(WindowFunction):

    def __init__(self):
        object.__init__(self)
        self.total = 0


    def add(self, value):
        if value == None:
            return
        self.total += value

    def sub(self, value):
        if value == None:
            return
        self.total -= value

    def end(self):
        return self.total
