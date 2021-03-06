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

import unittest

from bugzilla_etl import bz_etl, extract_bugzilla
from bugzilla_etl.alias_analysis import AliasAnalyzer
from bugzilla_etl.bz_etl import etl, etl_comments
from bugzilla_etl.extract_bugzilla import get_current_time
from mo_dots import Data
from mo_logs import startup, Log, constants
from mo_threads import ThreadedQueue
from pyLibrary import convert
from pyLibrary.sql.mysql import all_db, MySQL
from pyLibrary.testing import elasticsearch
from pyLibrary.testing.elasticsearch import FakeES
from test_etl import compare_both, MIN_TIMESTAMP, refresh_metadata
from util.database import make_test_instance


class TestExamples(unittest.TestCase):
    """
    USE THIS TO TEST A SPECIFIC SET OF BUGS FROM A LARGE BUGZILLA DATABASE
    I USE THIS TO IDENTIFY CANDIDATES TO ADD TO THE TEST SUITE
    """

    def setUp(self):
        self.settings = startup.read_settings(filename="tests/resources/config/test_examples.json")
        constants.set(self.settings.constants)
        Log.start(self.settings.debug)

        self.alias_analyzer = AliasAnalyzer(self.settings.alias)

    def tearDown(self):
        #CLOSE THE CACHED MySQL CONNECTIONS
        bz_etl.close_db_connections()

        if all_db:
            Log.error("not all db connections are closed")

        Log.stop()

    def test_specific_bugs(self):
        """
        USE A MYSQL DATABASE TO FILL AN ES INSTANCE (USE Fake_ES() INSTANCES TO KEEP
        THIS TEST LOCAL) WITH VERSIONS OF BUGS FROM settings.param.bugs.  COMPARE
        THOSE VERSIONS TO A REFERENCE ES (ALSO CHECKED INTO REPOSITORY)
        """
        reference = FakeES(self.settings.reference)
        candidate = elasticsearch.make_test_instance(self.settings.bugs)
        candidate_comments = elasticsearch.make_test_instance(self.settings.comments)

        make_test_instance(self.settings.bugzilla)
        with MySQL(self.settings.bugzilla) as db:

            # SETUP RUN PARAMETERS
            param = Data()
            param.end_time = convert.datetime2milli(get_current_time(db))
            param.start_time = MIN_TIMESTAMP
            param.start_time_str = extract_bugzilla.milli2string(db, MIN_TIMESTAMP)

            param.alias_file = self.settings.param.alias_file
            param.bug_list = self.settings.param.bugs
            param.allow_private_bugs = self.settings.param.allow_private_bugs

            with ThreadedQueue("etl queue", candidate, batch_size=1000) as output:
                etl(db, output, param, self.alias_analyzer, please_stop=None)
            with ThreadedQueue("etl queue", candidate_comments, batch_size=1000) as output:
                etl_comments(db, output, param, please_stop=None)

        # COMPARE ALL BUGS
        refresh_metadata(candidate)
        compare_both(candidate, reference, self.settings, self.settings.param.bugs)
