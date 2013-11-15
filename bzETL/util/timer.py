# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
import time
from .strings import expand_template
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
        self.description=expand_template(description, param)  #WE WOULD LIKE TO KEEP THIS TEMPLATE, AND PASS IT TO THE LOGGER ON __exit__(), WE FAKE IT FOR NOW

    def __enter__(self):
        Log.note("Timer start: {{description}}", {
            "description":self.description
        })


        self.start = time.clock()
        return self

    def __exit__(self, type, value, traceback):
        self.end = time.clock()
        self.interval = self.end - self.start
        Log.note("Timer end  : {{description}} (took {{duration}} sec)", {
            "description":self.description,
            "duration":round(self.interval, 3)
        })


        
    @property
    def duration(self):
        return self.interval