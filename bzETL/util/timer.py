# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from datetime import timedelta
import time
from . import struct
from .struct import nvl
from .logs import Log


class Timer:
    """
    USAGE:
    with Timer("doing hard time"):
        something_that_takes_long()
    OUTPUT:
        doing hard time took 45.468 sec
    """

    def __init__(self, description, param=None):
        self.template = description
        self.param = nvl(param, {})

    def __enter__(self):
        Log.note("Timer start: " + self.template, self.param)

        self.start = time.clock()
        return self

    def __exit__(self, type, value, traceback):
        self.end = time.clock()
        self.interval = self.end - self.start
        param = struct.wrap(self.param)
        param.duration = timedelta(seconds=self.interval)
        Log.note("Timer end  : " + self.template + " (took {{duration}})", self.param)

    @property
    def duration(self):
        return self.interval
