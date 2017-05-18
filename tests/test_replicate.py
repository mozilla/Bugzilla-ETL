# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from bzETL import replicate
from pyLibrary import convert
from pyLibrary.debugs import startup
from pyLibrary.debugs.logs import Log
from pyLibrary.env import elasticsearch


def test_replication():
    try:
        settings=startup.read_settings(filename="replication_settings.json")
        Log.start(settings.debug)

        source=elasticsearch.Index(settings.source)
        destination=replicate.get_or_create_index(settings["destination"], source)

        replicate.replicate(source, destination, [537285], convert.string2datetime("19900101", "%Y%m%d"))
    finally:
        Log.stop()


if __name__=="__main__":
    test_replication()
