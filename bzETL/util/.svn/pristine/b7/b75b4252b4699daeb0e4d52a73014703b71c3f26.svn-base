################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################
import time
from .debug import D


## USAGE:
## with Timer("doing hard time"):
##     something_that_takes_long()
##
## OUTPUT:
##     doing hard time took 45.468 sec


class Timer:

    def __init__(self, description):
        self.description=description

    def __enter__(self):
        self.start = time.clock()
        return self

    def __exit__(self, type, value, traceback):
        self.end = time.clock()
        self.interval = self.end - self.start
        D.println("{{description}} took {{duration}} sec", {
            "description":self.description,
            "duration":round(self.interval, 3)
        })


        
    @property
    def duration(self):
        return self.interval