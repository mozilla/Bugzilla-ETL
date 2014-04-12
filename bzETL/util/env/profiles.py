# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals
from datetime import datetime
from time import clock

ON = False
profiles = {}


class Profiler(object):
    """

    """

    def __new__(cls, *args):
        if ON:
            output = profiles.get(args[0], None)
            if output:
                return output
        output = object.__new__(cls, *args)
        return output

    def __init__(self, description):
        if ON and not hasattr(self, "description"):
            self.description = description
            self.num = 0
            self.total = 0
            profiles[description] = self

    def __enter__(self):
        if ON:
            self.start = clock()
        return self

    def __exit__(self, type, value, traceback):
        if ON:
            self.end = clock()
            self.total += self.end - self.start
            self.num += 1


def write(profile_settings):
    from ..cnv import CNV
    from .files import File

    stats = [{
        "description": p.description,
        "num_calls": p.num,
        "total_time": p.total,
        "total_time_per_call": p.total / p.num
    }
        for p in profiles.values() if p.num > 0
    ]
    stats_file = File(profile_settings.filename, suffix=CNV.datetime2string(datetime.now(), "_%Y%m%d_%H%M%S"))
    if stats:
        stats_file.write(CNV.list2tab(stats))
    else:
        stats_file.write("<no profiles>")

