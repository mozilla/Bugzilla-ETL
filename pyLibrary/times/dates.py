# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
#
# MONKEY-PATCH datetime FOR MORE AWESOME FUN
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals
from __future__ import division

from datetime import datetime, date
import math


class Date(object):

    MIN = datetime(1, 1, 1)
    MAX = datetime(2286, 11, 20, 17, 46, 39)

    def __init__(self, *args):
        try:
            if len(args) == 1:
                a0 = args[0]
                if isinstance(a0, (datetime, date)):
                    self.value = a0
                elif isinstance(a0, Date):
                    self.value = a0.value
                elif isinstance(a0, (int, long, float)):
                    if a0 == 9999999999000:  # PYPY BUG https://bugs.pypy.org/issue1697
                        self.value = Date.MAX
                    else:
                        self.value = datetime.utcfromtimestamp(a0/1000)
                elif isinstance(a0, basestring):
                    self.value = unicode2datetime(a0)
                else:
                    self.value = datetime(*args)
            else:
                if isinstance(args[0], basestring):
                    self.value = unicode2datetime(*args)
                else:
                    self.value = datetime(*args)

        except Exception, e:
            Log.error("Can not convert {{args}} to Date", {"args": args}, e)

    def floor(self, duration=None):
        if duration is None:  # ASSUME DAY
            return Date(math.floor(self.milli / 86400000) * 86400000)
        elif not duration.month:
            return Date(math.floor(self.milli / duration.milli) * duration.milli)
        else:
            month = math.floor(self.value.month / duration.month) * duration.month
            return Date(datetime(self.value.year, month, 1))

    def format(self, format="%Y-%m-%d %H:%M:%S"):
        try:
            return self.value.strftime(format)
        except Exception, e:
            Log.error("Can not format {{value}} with {{format}}", {"value": self.value, "format": format}, e)

    @property
    def milli(self):
        try:
            if self.value == None:
                return None
            elif isinstance(self.value, datetime):
                epoch = datetime(1970, 1, 1)
            elif isinstance(self.value, date):
                epoch = date(1970, 1, 1)
            else:
                Log.error("Can not convert {{value}} of type {{type}}", {"value": self.value, "type": self.value.__class__})

            diff = self.value - epoch
            return long(diff.total_seconds()) * 1000L + long(diff.microseconds / 1000)
        except Exception, e:
            Log.error("Can not convert {{value}}", {"value": self.value}, e)

    @property
    def unix(self):
        return self.milli/1000

    def addDay(self):
        return Date(self.value + timedelta(days=1))

    def add(self, interval):
        return Date(self.value+interval)


    @staticmethod
    def now():
        return Date(datetime.utcnow())

    @staticmethod
    def eod():
        """
        RETURN END-OF-TODAY (WHICH IS SAME AS BEGINNING OF TOMORROW)
        """
        return Date.today().addDay()

    @staticmethod
    def today():
        return Date(datetime.utcnow()).floor()

    def __str__(self):
        return str(self.value)

    def __sub__(self, other):
        if isinstance(other, datetime):
            return self.value - other
        elif isinstance(other, date):
            return self.value - other
        elif isinstance(other, Date):
            return self.value - other.value
        else:
            Log.error("can not subtract {{type}} from Date", {"type":other.__class__.__name__})


def unicode2datetime(value, format=None):
    """
    CONVERT UNICODE STRING TO datetime VALUE
    """
    ## http://docs.python.org/2/library/datetime.html#strftime-and-strptime-behavior
    if value == None:
        return None

    if format != None:
        try:
            return datetime.strptime(value, format)
        except Exception, e:
            Log.error("Can not format {{value}} with {{format}}", {"value": value, "format": format}, e)

    formats = [
        "%Y%m%d",
        "%d%m%Y",
        "%d%m%y",
        "%d%b%Y",
        "%d%b%y",
        "%d%B%Y",
        "%d%B%y"
    ]
    value = deformat(value)
    for f in formats:
        try:
            return unicode2datetime(value, format=f)
        except Exception:
            pass
    else:
        Log.error("Can not interpret {{value}} as a datetime", {"value": value})




from ..env.logs import Log
