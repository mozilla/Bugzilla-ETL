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
from math import sqrt
from ..cnv import CNV
from ..collections import OR
from ..struct import nvl, Struct, Null
from ..env.logs import Log


DEBUG = True
EPSILON = 0.000001



def stats2z_moment(stats):
    # MODIFIED FROM http://statsmodels.sourceforge.net/devel/_modules/statsmodels/stats/moment_helpers.html
    # ADDED count
    mc0, mc1, mc2, skew, kurt = stats.count, nvl(stats.mean, 0), nvl(stats.variance, 0), nvl(stats.skew, 0), nvl(stats.kurtosis, 0)

    mz0 = mc0
    mz1 = mc1 * mc0
    mz2 = (mc2 + mc1 * mc1) * mc0
    mc3 = nvl(skew, 0) * (mc2 ** 1.5) # 3rd central moment
    mz3 = (mc3 + 3 * mc1 * mc2 + mc1 ** 3) * mc0  # 3rd non-central moment
    mc4 = (nvl(kurt, 0) + 3.0) * (mc2 ** 2.0) # 4th central moment
    mz4 = (mc4 + 4 * mc1 * mc3 + 6 * mc1 * mc1 * mc2 + mc1 ** 4) * mc0

    m = Z_moment(mz0, mz1, mz2, mz3, mz4)
    if DEBUG:
        globals()["DEBUG"] = False
        try:
            v = z_moment2stats(m, unbiased=False)
            assert closeEnough(v.count, stats.count)
            assert closeEnough(v.mean, stats.mean)
            assert closeEnough(v.variance, stats.variance)
            assert closeEnough(v.skew, stats.skew)
            assert closeEnough(v.kurtosis, stats.vkurtosis)
        except Exception, e:
            v = z_moment2stats(m, unbiased=False)
            Log.error("programmer error")
        globals()["DEBUG"] = True
    return m


def closeEnough(a, b):
    if a == None and b == None:
        return True
    if a == None or b == None:
        return False

    if abs(a - b) <= EPSILON * (abs(a) + abs(b) + 1):
        return True
    return False


def z_moment2stats(z_moment, unbiased=True):
    Z = z_moment.S
    N = Z[0]
    if N == 0:
        return Stats()

    mean = Z[1] / N
    Z2 = Z[2] / N
    Z3 = Z[3] / N
    Z4 = Z[4] / N

    if N == 1:
        variance = None
        skew = None
        kurtosis = None
    else:
        variance = (Z2 - mean * mean)
        error = -EPSILON * (abs(Z2) + 1)  # EXPECTED FLOAT ERROR

        if error < variance <= 0:  # TODO: MAKE THIS A TEST ON SIGNIFICANT DIGITS
            skew = None
            kurtosis = None
        elif variance < error:
            Log.error("variance can not be negative ({{var}})", {"var": variance})
        else:
            mc3 = (Z3 - (3 * mean * variance + mean ** 3))  # 3rd central moment
            mc4 = (Z4 - (4 * mean * mc3 + 6 * mean * mean * variance + mean ** 4))
            skew = mc3 / (variance ** 1.5)
            kurtosis = (mc4 / (variance ** 2.0)) - 3.0

    stats = Stats(
        count=N,
        mean=mean,
        variance=variance,
        skew=skew,
        kurtosis=kurtosis,
        unbiased=unbiased
    )

    if DEBUG:
        globals()["DEBUG"] = False
        v=Null
        try:
            v = stats2z_moment(stats)
            for i in range(5):
                assert closeEnough(v.S[i], Z[i])
        except Exception, e:
            Log.error("Convertion failed.  Programmer error:\nfrom={{from|indent}},\nresult stats={{stats|indent}},\nexpected parem={{expected|indent}}", {
                "from": Z,
                "stats": stats,
                "expected": v.S
            }, e)
        globals()["DEBUG"] = True

    return stats


class Stats(Struct):
    def __init__(self, **kwargs):
        Struct.__init__(self)

        if "samples" in kwargs:
            s = z_moment2stats(Z_moment.new_instance(kwargs["samples"]))
            self.count = s.count
            self.mean = s.mean
            self.variance = s.variance
            self.skew = s.skew
            self.kurtosis = s.kurtosis
            return

        if "count" not in kwargs:
            self.count = 0
            self.mean = None
            self.variance = None
            self.skew = None
            self.kurtosis = None
        elif "mean" not in kwargs:
            self.count = kwargs["count"]
            self.mean = None
            self.variance = None
            self.skew = None
            self.kurtosis = None
        elif "variance" not in kwargs and "std" not in kwargs:
            self.count = kwargs["count"]
            self.mean = kwargs["mean"]
            self.variance = 0
            self.skew = None
            self.kurtosis = None
        elif "skew" not in kwargs:
            self.count = kwargs["count"]
            self.mean = kwargs["mean"]
            self.variance = kwargs["variance"] if "variance" in kwargs else kwargs["std"] ** 2
            self.skew = None
            self.kurtosis = None
        elif "kurtosis" not in kwargs:
            self.count = kwargs["count"]
            self.mean = kwargs["mean"]
            self.variance = kwargs["variance"] if "variance" in kwargs else kwargs["std"] ** 2
            self.skew = kwargs["skew"]
            self.kurtosis = None
        else:
            self.count = kwargs["count"]
            self.mean = kwargs["mean"]
            self.variance = kwargs["variance"] if "variance" in kwargs else kwargs["std"] ** 2
            self.skew = kwargs["skew"]
            self.kurtosis = kwargs["kurtosis"]

        self.unbiased = \
            kwargs["unbiased"] if "unbiased" in kwargs else \
                not kwargs["biased"] if "biased" in kwargs else \
                    False


    @property
    def std(self):
        return sqrt(self.variance)


class Z_moment(object):
    """
    ZERO-CENTERED MOMENTS
    """

    def __init__(self, *args):
        self.S = tuple(args)

    def __add__(self, other):
        return Z_moment(*map(add, self.S, other.S))

    def __sub__(self, other):
        return Z_moment(*map(sub, self.S, other.S))

    @property
    def tuple(self):
    #RETURN AS ORDERED TUPLE
        return self.S

    @property
    def dict(self):
    #RETURN HASH OF SUMS
        return {u"s" + unicode(i): m for i, m in enumerate(self.S)}


    @staticmethod
    def new_instance(values=None):
        if values == None:
            return Z_moment()
        values = [float(v) for v in values if v != None]

        return Z_moment(
            len(values),
            sum([n for n in values]),
            sum([pow(n, 2) for n in values]),
            sum([pow(n, 3) for n in values]),
            sum([pow(n, 4) for n in values])
        )


def add(a, b):
    return nvl(a, 0) + nvl(b, 0)


def sub(a, b):
    return nvl(a, 0) - nvl(b, 0)


def z_moment2dict(z):
    #RETURN HASH OF SUMS
    return {u"s" + unicode(i): m for i, m in enumerate(z.S)}


setattr(CNV, "z_moment2dict", staticmethod(z_moment2dict))


def median(values, simple=True, mean_weight=0.0):
    """
    RETURN MEDIAN VALUE

    IF simple=False THEN IN THE EVENT MULTIPLE INSTANCES OF THE
    MEDIAN VALUE, THE MEDIAN IS INTERPOLATED BASED ON ITS POSITION
    IN THE MEDIAN RANGE

    mean_weight IS TO PICK A MEDIAN VALUE IN THE ODD CASE THAT IS
    CLOSER TO THE MEAN (PICK A MEDIAN BETWEEN TWO MODES IN BIMODAL CASE)
    """

    if OR(v == None for v in values):
        Log.error("median is not ready to handle None")

    try:
        if not values:
            return Null

        l = len(values)
        _sorted = sorted(values)

        middle = l / 2
        _median = float(_sorted[middle])

        if simple:
            if l % 2 == 0:
                return float(_sorted[middle - 1] + _median) / 2
            return _median

        #FIND RANGE OF THE median
        start_index = middle - 1
        while start_index > 0 and _sorted[start_index] == _median:
            start_index -= 1
        start_index += 1
        stop_index = middle + 1
        while stop_index < l and _sorted[stop_index] == _median:
            stop_index += 1

        num_middle = stop_index - start_index

        if l % 2 == 0:
            if num_middle == 1:
                return float(_sorted[middle - 1] + _median) / 2
            else:
                return (_median - 0.5) + float(middle - start_index) / float(num_middle)
        else:
            if num_middle == 1:
                return (1 - mean_weight) * _median + mean_weight * (_sorted[middle - 1] + _sorted[middle + 1]) / 2
            else:
                return (_median - 0.5) + float(middle + 0.5 - start_index) / float(num_middle)
    except Exception, e:
        Log.error("problem with median of {{values}}", {"values": values}, e)


zero = Stats()
