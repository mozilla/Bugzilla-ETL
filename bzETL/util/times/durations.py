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

from .. import regex
from ..cnv import CNV
from ..collections import MIN
from ..env.logs import Log
from ..maths import Math
from ..struct import wrap


class Duration(object):

    def __new__(cls, obj=None):
        output = object.__new__(cls)
        if obj == None:
            return None
        if Math.is_number(obj):
            output.milli = obj
            output.month = 0
            return output
        elif isinstance(obj, basestring):
            return parse(obj)
        elif isinstance(obj, Duration):
            output.milli = obj.milli
            output.month = obj.month
            return output
        elif Math.is_nan(obj):
            return None
        else:
            Log.error("Do not know type of object (" + CNV.object2JSON(obj) + ")of to make a Duration")


    def __add__(self, other):
        output = Duration(0)
        output.milli = self.milli + other.milli
        output.month = self.month + other.month
        return output

    def __mul__(self, amount):
        output = Duration(0)
        output.milli = self.milli * amount
        output.month = self.month * amount
        return output

    def __rmul__(self, amount):
        output = Duration(0)
        output.milli = self.milli * amount
        output.month = self.month * amount
        return output

    def __div__(self, amount):
        if isinstance(amount, Duration) and not amount.month:
            m = self.month
            r = self.milli

            # DO NOT CONSIDER TIME OF DAY
            tod = r % MILLI_VALUES.day
            r = r - tod

            if m == 0 and r > (MILLI_VALUES.year / 3):
                m = Math.floor(12 * self.milli / MILLI_VALUES.year)
                r -= (m / 12) * MILLI_VALUES.year
            else:
                r = r - (self.month * MILLI_VALUES.month)
                if r >= MILLI_VALUES.day * 31:
                    Log.error("Do not know how to handle")
            r = MIN(29 / 30, (r + tod) / (MILLI_VALUES.day * 30))

            output = Math.floor(m / amount.month) + r
            return output
        elif Math.is_number(amount):
            output = Duration(0)
            output.milli = self.milli / amount
            output.month = self.month / amount
            return output
        else:
            return self.milli / amount.milli


    def __sub__(self, duration):
        output = Duration(0)
        output.milli = self.milli - duration.milli
        output.month = self.month - duration.month
        return output

    def floor(self, interval=None):
        if not isinstance(interval, Duration):
            Log.error("Expecting an interval as a Duration object")

        output = Duration(0)
        if interval.month:
            if self.month:
                output.month = Math.floor(self.month / interval.month) * interval.month
                output.milli = output.month * MILLI_VALUES.month
                return output

            # A MONTH OF DURATION IS BIGGER THAN A CANONICAL MONTH
            output.month = Math.floor(self.milli * 12 / MILLI_VALUES["year"] / interval.month) * interval.month
            output.milli = output.month * MILLI_VALUES.month
        else:
            output.milli = Math.floor(self.milli / (interval.milli)) * (interval.milli)
        return output

    def __str__(self):
        if not self.milli:
            return "zero"

        output = ""
        rest = (self.milli - (MILLI_VALUES.month * self.month)) # DO NOT INCLUDE THE MONTH'S MILLIS
        isNegative = (rest < 0)
        rest = Math.abs(rest)

        # MILLI
        rem = rest % 1000
        if rem != 0:
            output = "+" + rem + "milli" + output
        rest = Math.floor(rest / 1000)

        # SECOND
        rem = rest % 60
        if rem != 0:
            output = "+" + rem + "second" + output
        rest = Math.floor(rest / 60)

        # MINUTE
        rem = rest % 60
        if rem != 0:
            output = "+" + rem + "minute" + output
        rest = Math.floor(rest / 60)

        # HOUR
        rem = rest % 24
        if rem != 0:
            output = "+" + rem + "hour" + output
        rest = Math.floor(rest / 24)

        # DAY
        if rest < 11 and rest != 7:
            rem = rest
            rest = 0
        else:
            rem = rest % 7
            rest = Math.floor(rest / 7)

        if rem != 0:
            output = "+" + rem + "day" + output

        # WEEK
        if rest != 0:
            output = "+" + rest + "week" + output

        if isNegative:
            output = output.replace("+", "-")

        # MONTH AND YEAR
        if self.month:
            sign = "-" if self.month < 0 else "+"
            month = Math.abs(self.month)

            if month <= 18 and month != 12:
                output = sign + month + "month" + output
            else:
                m = month % 12
                if m != 0:
                    output = sign + m + "month" + output
                y = Math.floor(month / 12)
                output = sign + y + "year" + output

        if output[0] == "+":
            output = output[1::]
        if output[0] == '1' and not Math.is_number(output[1]):
            output = output[1::]
        return output


    def format(self, interval, rounding):
        return self.round(Duration.newInstance(interval), rounding) + interval

    def round(self, interval, rounding=0):
        output = self / interval
        output = Math.round(output, rounding)
        return output


def _string2Duration(text):
    """
    CONVERT SIMPLE <float><type> TO A DURATION OBJECT
    """
    if text == "" or text == "zero":
        return Duration(0)

    amount, interval = regex.match(r"([\d\.]*)(.*)", text)
    amount = CNV.value2int(amount) if amount else 0

    if MILLI_VALUES[interval] == None:
        Log.error(interval + " is not a recognized duration type (did you use the pural form by mistake?")

    output = Duration(0)
    if MONTH_VALUES[interval] == 0:
        output.milli = amount * MILLI_VALUES[interval]
    else:
        output.milli = amount * MONTH_VALUES[interval] * MILLI_VALUES.month
        output.month = amount * MONTH_VALUES[interval]

    return output


def parse(value):
    output = Duration(0)

    # EXPECTING CONCAT OF <sign><integer><type>
    plist = value.split("+")
    for p, pplist in enumerate(plist):
        mlist = pplist.split("-")
        output = output + _string2Duration(mlist[0])
        for m in mlist[1::]:
            output = output.subtract(_string2Duration(m))
    return output


MILLI_VALUES = wrap({
    "year": 52 * 7 * 24 * 60 * 60 * 1000, # 52weeks
    "quarter": 13 * 7 * 24 * 60 * 60 * 1000, # 13weeks
    "month": 28 * 24 * 60 * 60 * 1000, # 4weeks
    "week": 7 * 24 * 60 * 60 * 1000,
    "day": 24 * 60 * 60 * 1000,
    "hour": 60 * 60 * 1000,
    "minute": 60 * 1000,
    "second": 1000,
    "milli": 1
})

MONTH_VALUES = wrap({
    "year": 12,
    "quarter": 3,
    "month": 1,
    "week": 0,
    "day": 0,
    "hour": 0,
    "minute": 0,
    "second": 0,
    "milli": 0
})

# A REAL MONTH IS LARGER THAN THE CANONICAL MONTH
MONTH_SKEW = MILLI_VALUES["year"] / 12 - MILLI_VALUES.month


def compare(a, b):
    return a.milli - b.milli


DOMAIN = {
    "type": "duration",
    "compare": compare
}

ZERO = Duration(0)
SECOND = Duration("second")
MINUTE = Duration("minute")
HOUR = Duration("hour")
DAY = Duration("day")
WEEK = Duration("week")
MONTH = Duration("month")
QUARTER = Duration("quarter")
YEAR = Duration("year")

COMMON_INTERVALS = [
    Duration("second"),
    Duration("15second"),
    Duration("30second"),
    Duration("minute"),
    Duration("5minute"),
    Duration("15minute"),
    Duration("30minute"),
    Duration("hour"),
    Duration("2hour"),
    Duration("3hour"),
    Duration("6hour"),
    Duration("12hour"),
    Duration("day"),
    Duration("2day"),
    Duration("week"),
    Duration("2week"),
    Duration("month"),
    Duration("2month"),
    Duration("quarter"),
    Duration("6month"),
    Duration("year")
]
