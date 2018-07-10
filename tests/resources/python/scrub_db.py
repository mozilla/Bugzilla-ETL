# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from mo_logs import startup, Log
from pyLibrary.sql import SQL
from pyLibrary.sql.mysql import MySQL


def main():
    """
    MEANT TO BE RUN JUST ONCE IN DEVELOPMENT TO CONVERT A BIG PUBLIC
    DATABASE (8G+) INTO A TINY TESTING MySQL (FOR ADDING TO REPOSITORY)
    """
    try:
        settings=startup.read_settings()
        Log.start(settings.debug)
        input=raw_input("We are going to totally wipe out the "+settings.bugzilla.schema.upper()+" schema at "+settings.bugzilla.host.upper()+"!  Type \"YES\" to continue: ")
        if input!="YES":
            Log.note("Aborted.  No Changes made.")
            return

        Log.note("Scrubbing db of those pesky records.")
        Log.note("This is going to take hours ...")

        execute_file(settings.bugzilla, "./tests/resources/sql/scrub_db.sql", {
            "schema":settings.bugzilla.schema,
            "bug_list":SQL(settings.param.bugs)
        })
        Log.note("... Done!")
    finally:
        Log.stop()

main()
