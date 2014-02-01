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
import math
from ..struct import Null, nvl
from ..env.logs import Log
from ..strings import find_first
from ..collections.multiset import Multiset


class Math(object):
    @staticmethod
    def bayesian_add(*args):
        a = args[0]
        if a >= 1 or a <= 0:
            Log.error("Only allowed values *between* zero and one")

        for b in args[1:]:
            if b >= 1 or b <= 0:
                Log.error("Only allowed values *between* zero and one")
            a = a * b / (a * b + (1 - a) * (1 - b))

        return a

    @staticmethod
    def bayesian_subtract(a, b):
        return Math.bayesian_add(a, 1 - b)


    @staticmethod
    def abs(v):
        if v == None:
            return Null
        return abs(v)

    # FOR GOODNESS SAKE - IF YOU PROVIDE A METHOD abs(), PLEASE PROVIDE ITS COMPLEMENT
    # x = abs(x)*sign(x)
    # FOUND IN numpy, BUT WE USUALLY DO NOT NEED TO BRING IN A BIG LIB FOR A SIMPLE DECISION
    @staticmethod
    def sign(v):
        if v == None:
            return Null
        if v < 0:
            return -1
        if v > 0:
            return +1
        return 0


    @staticmethod
    def is_number(s):
        try:
            float(s)
            return True
        except Exception:
            return False

    @staticmethod
    def is_integer(s):
        try:
            if float(s) == round(float(s), 0):
                return True
            return False
        except Exception:
            return False

    @staticmethod
    def round_sci(value, decimal=None, digits=None):
        if digits != None:
            m = pow(10, math.floor(math.log10(digits)))
            return round(value / m, digits) * m

        return round(value, decimal)


    @staticmethod
    def floor(value, mod=None):
        """
        x == floor(x, a) + mod(x, a)  FOR ALL a
        """
        mod = nvl(mod, 1)
        v = int(math.floor(value))
        return v - (v % mod)


    #RETURN A VALUE CLOSE TO value, BUT WITH SHORTER len(unicode(value))<len(unicode(value)):
    @staticmethod
    def approx_str(value):
        v = unicode(value)
        d = v.find(".")
        if d == -1: return value

        i = find_first(v, ["9999", "0000"], d)
        if i == -1: return value

        return Math.round_sci(value, decimal=i - d - 1)


    @staticmethod
    def min(*values):
        if isinstance(values, tuple) and len(values) == 1 and isinstance(values[0], (list, set, tuple, Multiset)):
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


    @staticmethod
    def max(*values):
        if isinstance(values, tuple) and len(values) == 1 and isinstance(values[0], (list, set, tuple)):
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

    @staticmethod
    def ceiling(value):
        return int(math.ceil(value))

    @staticmethod
    def product(*values):
        if isinstance(values, tuple) and len(values) == 1 and isinstance(values[0], (list, set, tuple, Multiset)):
            values = values[0]
        output = 1
        for v in values:
            if v == None:
                continue
            if isinstance(v, float) and math.isnan(v):
                continue
            output = output * v
        return output

