# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import, division

import re

from mo_dots import Data, coalesce, is_data, listwrap, wrap_leaves
from mo_logs import Log, strings
from mo_times.dates import Date

GLOBALS = {
    "true": True,
    "false": False,
    "null": None,
    "EMPTY_DICT": {},
    "coalesce": coalesce,
    "listwrap": listwrap,
    "Date": Date,
    "Log": Log,
    "Data": Data,
    "re": re,
    "wrap_leaves": wrap_leaves,
    "is_data": is_data
}


def compile_expression(source):
    """
    THIS FUNCTION IS ON ITS OWN FOR MINIMAL GLOBAL NAMESPACE

    :param source:  PYTHON SOURCE CODE
    :return:  PYTHON FUNCTION
    """
    fake_locals = {}
    try:
        exec(
            (
                "def output(row, rownum=None, rows=None):\n" +
                "    _source = " + strings.quote(source) + "\n" +
                "    try:\n" +
                "        return " + source + "\n" +
                "    except Exception as e:\n" +
                "        Log.error(u'Problem with dynamic function {{func|quote}}',  func=_source, cause=e)\n"
            ),
            GLOBALS,
            fake_locals,
        )
    except Exception as e:
        Log.error(u"Bad source: {{source}}", source=source, cause=e)
    return fake_locals["output"]
