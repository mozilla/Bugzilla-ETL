# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
import math
from . import struct
from .struct import Null, nvl
from .logs import Log
from .strings import find_first

class Math(object):

    @staticmethod
    def bayesian_add(a, b):
        if a>=1 or b>=1 or a<=0 or b<=0: Log.error("Only allowed values *between* zero and one")
        return a*b/(a*b+(1-a)*(1-b))



    # FOR GOODNESS SAKE - IF YOU PROVIDE A METHOD abs(), PLEASE PROVIDE IT'S COMPLEMENT
    # x = abs(x)*sign(x)
    # FOUND IN numpy, BUT WE USUALLY DO NOT NEED TO BRING IN A BIG LIB FOR A SIMPLE DECISION
    @staticmethod
    def sign(v):
        if v<0: return -1
        if v>0: return +1
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
            if float(s)==round(float(s), 0):
                return True
            return False
        except Exception:
            return False

    @staticmethod
    def round_sci(value, decimal=None, digits=None):
        if digits != None:
            m=pow(10, math.floor(math.log10(digits)))
            return round(value/m, digits)*m

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
        v=unicode(value)
        d=v.find(".")
        if d==-1: return value

        i=find_first(v, ["9999", "0000"], d)
        if i==-1: return value

        return Math.round_sci(value, decimal=i-d-1)


    @staticmethod
    def min(values):
        output = Null
        for v in values:
            if v == None:
                continue
            if math.isnan(v):
                continue
            if output == None:
                output = v
                continue
            output = min(output, v)
        return output



    @staticmethod
    def max(values):
        output = Null
        for v in values:
            if v == None:
                continue
            if math.isnan(v):
                continue
            if output == None:
                output = v
                continue
            output = max(output, v)
        return output


