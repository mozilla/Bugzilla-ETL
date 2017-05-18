# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#





from pyLibrary.debugs.logs import Log
from pyLibrary.strings import expand_template


class SQL(str):
    """
    ACTUAL SQL, DO NOT QUOTE THIS STRING
    """
    def __init__(self, template='', param=None):
        str.__init__(self)
        self.template = template
        self.param = param

    @property
    def sql(self):
        return expand_template(self.template, self.param)

    def __str__(self):
        Log.error("do not do this")

