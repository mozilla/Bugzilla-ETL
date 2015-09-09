# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
import unittest
from bzETL import bz_etl, extract_bugzilla
from bzETL.bz_etl import etl
from bzETL.extract_bugzilla import get_current_time
from pyLibrary import convert
from pyLibrary.debugs import startup
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import Dict
from pyLibrary.sql.mysql import all_db, MySQL
from pyLibrary.testing import elasticsearch
from pyLibrary.thread.threads import ThreadedQueue


class TestOneETL(unittest.TestCase):
    """
    USE THIS TO TEST A SPECIFIC SET OF BUGS FROM A LARGE BUGZILLA DATABASE
    I USE THIS TO IDENTIFY CANDIDATES TO ADD TO THE TEST SUITE
    """
    def setUp(self):
        self.settings = startup.read_settings(filename="test_one_settings.json")
        Log.start(self.settings.debug)


    def tearDown(self):
        #CLOSE THE CACHED MySQL CONNECTIONS
        bz_etl.close_db_connections()

        if all_db:
            Log.error("not all db connections are closed")

        Log.stop()


    def test_specific_bugs(self):
        """
        USE A MYSQL DATABASE TO FILL AN ES INSTANCE (USE Fake_ES() INSTANCES TO KEEP
        THIS TEST LOCAL) WITH VERSIONS OF BUGS FROM settings.param.bugs.
        """
        with MySQL(self.settings.bugzilla) as db:
            candidate = elasticsearch.make_test_instance("candidate", self.settings.elasticsearch)

            #SETUP RUN PARAMETERS
            param = Dict()
            param.end_time = convert.datetime2milli(get_current_time(db))
            param.start_time = 0
            param.start_time_str = extract_bugzilla.milli2string(db, 0)

            param.alias_file = self.settings.param.alias_file
            param.bug_list = self.settings.param.bugs
            param.allow_private_bugs = self.settings.param.allow_private_bugs

            with ThreadedQueue(candidate, size=1000) as output:
                etl(db, output, param, please_stop=None)


        #TODO: INCLUDE OPTION TO USE REAL ES (AND ENSURE REALLY WORKING)
        # es_settings=Dict(**{
        #     "host": "http://localhost",
        #     "port": "9200",
        #     "index": ElasticSearch.proto_name("test_public_bugs"),
        #     "type": "bug_version",
        #     "schema_file": "./resources/json/bug_version.json"
        # })
        # es = ElasticSearch.create_index(es_settings, File(es_settings.schema_file).read())
        # es.delete_all_but("test_public_bugs", es_settings.index)
