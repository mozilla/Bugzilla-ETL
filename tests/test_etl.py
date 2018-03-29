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
from datetime import datetime

from mo_future import text_type

from bzETL import extract_bugzilla, bz_etl
from bzETL.bz_etl import etl,  MIN_TIMESTAMP
from bzETL.extract_bugzilla import get_current_time, SCREENED_WHITEBOARD_BUG_GROUPS
from jx_python import jx
from mo_dots import Data, Null, wrap
from mo_files import File
from mo_json import json2value, value2json
from mo_logs import startup, constants, Log
from mo_math import MIN
from mo_math.randoms import Random
from mo_threads import ThreadedQueue, Till
from mo_times import Timer
from pyLibrary import convert
from pyLibrary.queries.jx_usingMySQL import esfilter2sqlwhere
from pyLibrary.sql.mysql import all_db, MySQL
from pyLibrary.testing import elasticsearch
from util import database, compare_es
from util.compare_es import get_all_bug_versions
from util.database import diff

BUG_GROUP_FOR_TESTING = "super secret"

class TestETL(unittest.TestCase):
    def setUp(self):
        self.settings = startup.read_settings(filename="./tests/resources/config/test_settings.json")
        constants.set(self.settings.constants)
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
        THIS TEST LOCAL) WITH VERSIONS OF BUGS FROM settings.param.bugs.  COMPARE
        THOSE VERSIONS TO A REFERENCE ES (ALSO CHECKED INTO REPOSITORY)
        """
        # settings.param.allow_private_bugs = True
        database.make_test_instance(self.settings.bugzilla)

        with MySQL(self.settings.bugzilla) as db:
            candidate = elasticsearch.make_test_instance("candidate", self.settings.candidate)
            reference = elasticsearch.open_test_instance("reference", self.settings.private_bugs_reference)

            #SETUP RUN PARAMETERS
            param = Data()
            param.end_time = convert.datetime2milli(get_current_time(db))
            param.start_time = MIN_TIMESTAMP
            param.start_time_str = extract_bugzilla.milli2string(db, MIN_TIMESTAMP)

            param.alias_file = self.settings.param.alias_file
            param.bug_list = self.settings.param.bugs
            param.allow_private_bugs = self.settings.param.allow_private_bugs

            with ThreadedQueue("etl_queue", candidate, batch_size=1000) as output:
                etl(db, output, param, self.settings.alias, please_stop=None)

            #COMPARE ALL BUGS
            Till(seconds=2).wait()  # MUST SLEEP WHILE ES DOES ITS INDEXING
            compare_both(candidate, reference, self.settings, self.settings.param.bugs)



    def random_sample_of_bugs(self):
        """
        I USE THIS TO FIND BUGS THAT CAUSE MY CODE PROBLEMS.  OF COURSE, IT ONLY WORKS
        WHEN I HAVE A REFERENCE TO COMPARE TO
        """
        NUM_TO_TEST = 100
        MAX_BUG_ID = 900000

        with MySQL(self.settings.bugzilla) as db:
            candidate = elasticsearch.make_test_instance("candidate", self.settings.candidate)
            reference = elasticsearch.Index(self.settings.private_bugs_reference)

            #GO FASTER BY STORING LOCAL FILE
            local_cache = File(self.settings.param.temp_dir + "/private_bugs.json")
            if local_cache.exists:
                private_bugs = set(json2value(local_cache.read()))
            else:
                with Timer("get private bugs"):
                    private_bugs = compare_es.get_private_bugs(reference)
                    local_cache.write(value2json(private_bugs))

            while True:
                some_bugs = [b for b in [Random.int(MAX_BUG_ID) for i in range(NUM_TO_TEST)] if b not in private_bugs]

                Log.note("Test with the following bug_ids: {{bugs}}", bugs=some_bugs)

                #SETUP RUN PARAMETERS
                param = Data()
                param.end_time = convert.datetime2milli(get_current_time(db))
                param.start_time = 0
                param.start_time_str = extract_bugzilla.milli2string(db, MIN_TIMESTAMP)
                param.alias_file = self.settings.param.alias_file

                try:
                    with ThreadedQueue("etl queue", candidate, batch_size=100) as output:
                        etl(db, output, param, self.settings.alias, please_stop=None)

                    #COMPARE ALL BUGS
                    Till(seconds=2).wait()  # MUST SLEEP WHILE ES DOES ITS INDEXING
                    found_errors = compare_both(candidate, reference, self.settings, some_bugs)
                    if found_errors:
                        Log.note("Errors found")
                        break
                    else:
                        pass
                except Exception as e:
                    Log.warning("Total failure during compare of bugs {{bugs}}", {"bugs": some_bugs}, e)

    def test_private_etl(self):
        """
        ENSURE IDENTIFIABLE INFORMATION DOES NOT EXIST ON ANY BUGS
        """
        File(self.settings.param.first_run_time).delete()
        File(self.settings.param.last_run_time).delete()
        self.settings.param.allow_private_bugs = True

        database.make_test_instance(self.settings.bugzilla)
        es = elasticsearch.make_test_instance("candidate", self.settings.fake.bugs)
        es_comments = elasticsearch.make_test_instance("candidate_comments", self.settings.fake.comments)
        bz_etl.main(self.settings, es, es_comments)

        ref = elasticsearch.open_test_instance("reference", self.settings.private_bugs_reference)
        compare_both(es, ref, self.settings, self.settings.param.bugs)

        #DIRECT COMPARE THE FILE JSON
        can = File(self.settings.fake.comments.filename).read()
        ref = File(self.settings.private_comments_reference.filename).read()
        if can != ref:
            for i, c in enumerate(can):
                found = -1
                if can[i] != ref[i]:
                    found = i
                    break
            Log.error("Comments do not match reference\n{{sample}}", {"sample": can[MIN([0, found - 100]):found + 100]})

    def test_public_etl(self):
        """
        ENSURE ETL GENERATES WHAT'S IN THE REFERENCE FILE
        """
        File(self.settings.param.first_run_time).delete()
        File(self.settings.param.last_run_time).delete()
        self.settings.param.allow_private_bugs = Null

        database.make_test_instance(self.settings.bugzilla)
        es = elasticsearch.make_test_instance("candidate", self.settings.fake.bugs)
        es_comments = elasticsearch.make_test_instance("candidate_comments", self.settings.fake.comments)
        bz_etl.main(self.settings, es, es_comments)

        ref = elasticsearch.open_test_instance("reference", self.settings.public_bugs_reference)
        compare_both(es, ref, self.settings, self.settings.param.bugs)

        #DIRECT COMPARE THE FILE JSON
        can = File(self.settings.fake.comments.filename).read()
        ref = File(self.settings.public_comments_reference.filename).read()
        if can != ref:
            found = -1
            for i, c in enumerate(can):
                if can[i] != ref[i]:
                    found = i
                    break
            Log.error("Comments do not match reference\n{{sample}}", {"sample": can[MIN(0, found - 100):found + 100:]})

    def test_private_bugs_do_not_show(self):
        self.settings.param.allow_private_bugs = False
        File(self.settings.param.first_run_time).delete()
        File(self.settings.param.last_run_time).delete()

        private_bugs = set(Random.sample(self.settings.param.bugs, 3))
        Log.note("The private bugs for this test are {{bugs}}", bugs= private_bugs)

        database.make_test_instance(self.settings.bugzilla)

        #MARK SOME BUGS PRIVATE
        with MySQL(self.settings.bugzilla) as db:
            for b in private_bugs:
                database.add_bug_group(db, b, BUG_GROUP_FOR_TESTING)

        es = elasticsearch.make_test_instance("candidate", self.settings.real.bugs)
        es_c = elasticsearch.make_test_instance("candidate_comments", self.settings.real.comments)
        bz_etl.main(self.settings, es, es_c)

        Till(seconds=2).wait()  # MUST SLEEP WHILE ES DOES ITS INDEXING
        verify_no_private_bugs(es, private_bugs)

    def test_recent_private_stuff_does_not_show(self):
        self.settings.param.allow_private_bugs = False
        File(self.settings.param.first_run_time).delete()
        File(self.settings.param.last_run_time).delete()

        database.make_test_instance(self.settings.bugzilla)

        es = elasticsearch.make_test_instance("candidate", self.settings.real.bugs)
        es_c = elasticsearch.make_test_instance("candidate_comments", self.settings.real.comments)
        bz_etl.main(self.settings, es, es_c)

        #MARK SOME STUFF PRIVATE
        with MySQL(self.settings.bugzilla) as db:
            #BUGS
            private_bugs = set(Random.sample(self.settings.param.bugs, 3))
            Log.note("The private bugs are {{bugs}}", bugs= private_bugs)
            for b in private_bugs:
                database.add_bug_group(db, b, BUG_GROUP_FOR_TESTING)

            #COMMENTS
            comments = db.query("SELECT comment_id FROM longdescs").comment_id
            marked_private_comments = Random.sample(comments, 5)
            for c in marked_private_comments:
                database.mark_comment_private(db, c, isprivate=1)

            #INCLUDE COMMENTS OF THE PRIVATE BUGS
            implied_private_comments = db.query("""
                SELECT comment_id FROM longdescs WHERE {{where}}
            """, {
                "where": esfilter2sqlwhere(db, {"terms":{"bug_id":private_bugs}})
            }).comment_id
            private_comments = marked_private_comments + implied_private_comments
            Log.note("The private comments are {{comments}}", comments= private_comments)

            #ATTACHMENTS
            attachments = db.query("SELECT bug_id, attach_id FROM attachments")
            private_attachments = Random.sample(attachments, 5)
            Log.note("The private attachments are {{attachments}}", attachments= private_attachments)
            for a in private_attachments:
                database.mark_attachment_private(db, a.attach_id, isprivate=1)

        if not File(self.settings.param.last_run_time).exists:
            Log.error("last_run_time should exist")
        bz_etl.main(self.settings, es, es_c)

        Till(seconds=2).wait()  # MUST SLEEP WHILE ES DOES ITS INDEXING
        verify_no_private_bugs(es, private_bugs)
        verify_no_private_attachments(es, private_attachments)
        verify_no_private_comments(es_c, private_comments)

        #MARK SOME STUFF PUBLIC

        with MySQL(self.settings.bugzilla) as db:
            for b in private_bugs:
                database.remove_bug_group(db, b, BUG_GROUP_FOR_TESTING)

        bz_etl.main(self.settings, es, es_c)

        #VERIFY BUG IS PUBLIC, BUT PRIVATE ATTACHMENTS AND COMMENTS STILL NOT
        Till(seconds=2).wait()  # MUST SLEEP WHILE ES DOES ITS INDEXING
        verify_public_bugs(es, private_bugs)
        verify_no_private_attachments(es, private_attachments)
        verify_no_private_comments(es_c, marked_private_comments)

    def test_private_attachments_do_not_show(self):
        self.settings.param.allow_private_bugs = False
        database.make_test_instance(self.settings.bugzilla)

        #MARK SOME STUFF PRIVATE
        with MySQL(self.settings.bugzilla) as db:
            private_attachments = db.query("""
                SELECT
                    bug_id,
                    attach_id
                FROM
                    attachments
                ORDER BY
                    mod(attach_id, 7),
                    attach_id
                LIMIT
                    5
            """)

            for a in private_attachments:
                database.mark_attachment_private(db, a.attach_id, isprivate=1)

        es = elasticsearch.make_test_instance("candidate", self.settings.real.bugs)
        es_c = elasticsearch.make_test_instance("candidate_comments", self.settings.real.comments)
        bz_etl.main(self.settings, es, es_c)

        Till(seconds=2).wait()  # MUST SLEEP WHILE ES DOES ITS INDEXING
        verify_no_private_attachments(es, private_attachments)

    def test_private_comments_do_not_show(self):
        self.settings.param.allow_private_bugs = False
        database.make_test_instance(self.settings.bugzilla)

        #MARK SOME COMMENTS PRIVATE
        with MySQL(self.settings.bugzilla) as db:
            private_comments = db.query("""
                SELECT
                    bug_id,
                    comment_id
                FROM
                    longdescs
                ORDER BY
                    mod(comment_id, 7),
                    comment_id
                LIMIT
                    5
            """)

            for c in private_comments:
                database.mark_comment_private(db, c.comment_id, 1)

        es = elasticsearch.make_test_instance("candidate", self.settings.real.bugs)
        es_c = elasticsearch.make_test_instance("candidate_comments", self.settings.real.comments)
        bz_etl.main(self.settings, es, es_c)

        Till(seconds=2).wait()  # MUST SLEEP WHILE ES DOES ITS INDEXING
        verify_no_private_comments(es, private_comments)

    def test_changes_to_private_bugs_still_have_bug_group(self):
        self.settings.param.allow_private_bugs = True
        File(self.settings.param.first_run_time).delete()
        File(self.settings.param.last_run_time).delete()

        private_bugs = set(Random.sample(self.settings.param.bugs, 3))

        Log.note("The private bugs for this test are {{bugs}}", bugs= private_bugs)

        database.make_test_instance(self.settings.bugzilla)

        #MARK SOME BUGS PRIVATE
        with MySQL(self.settings.bugzilla) as db:
            for b in private_bugs:
                database.add_bug_group(db, b, BUG_GROUP_FOR_TESTING)

        es = elasticsearch.make_test_instance("candidate", self.settings.real.bugs)
        es_c = elasticsearch.make_test_instance("candidate_comments", self.settings.real.comments)
        bz_etl.main(self.settings, es, es_c)

        # MAKE A CHANGE TO THE PRIVATE BUGS
        with MySQL(self.settings.bugzilla) as db:
            for b in private_bugs:
                with db:
                    old_bug = db.query("SELECT * FROM bugs WHERE bug_id={{bug_id}}", {"bug_id": b})[0]
                    new_bug = old_bug.copy()

                new_bug.bug_status = "NEW STATUS"
                diff(db, "bugs", old_bug, new_bug)


        #RUN INCREMENTAL
        bz_etl.main(self.settings, es, es_c)

        #VERIFY BUG GROUP STILL EXISTS
        Till(seconds=2).wait()  # MUST SLEEP WHILE ES DOES ITS INDEXING
        now = datetime.utcnow()
        results = es.search({
            "query": {"filtered": {
                "query": {"match_all": {}},
                "filter": {"and": [
                    {"terms": {"bug_id": private_bugs}},
                    {"range": {"expires_on": {"gte": convert.datetime2milli(now)}}}
                ]}
            }},
            "from": 0,
            "size": 200000,
            "sort": []
        })
        latest_bugs = jx.select(results.hits.hits, "_source")
        latest_bugs_index = jx.unique_index(latest_bugs, "bug_id")  # IF NOT UNIQUE, THEN ETL IS WRONG

        for bug_id in private_bugs:
            if latest_bugs_index[bug_id] == None:
                Log.error("Expecting to find the private bug {{bug_id}}", {"bug_id": bug_id})

            bug_group = latest_bugs_index[bug_id].bug_group
            if not bug_group:
                Log.error("Expecting private bug ({{bug_id}}) to have a bug group", {"bug_id": bug_id})
            if BUG_GROUP_FOR_TESTING not in bug_group:
                Log.error("Expecting private bug ({{bug_id}}) to have a \"{{bug_group}}\" bug group", {
                    "bug_id": bug_id,
                    "bug_group": BUG_GROUP_FOR_TESTING
                })

    def test_incremental_etl_catches_tracking_flags(self):
        database.make_test_instance(self.settings.bugzilla)

        with MySQL(self.settings.bugzilla) as db:
            es = elasticsearch.make_test_instance("candidate", self.settings.candidate)

            #SETUP RUN PARAMETERS
            param = Data()
            param.end_time = convert.datetime2milli(get_current_time(db))
            # FLAGS ADDED TO BUG 813650 ON 18/12/2012 2:38:08 AM (PDT), SO START AT SOME LATER TIME
            param.start_time = convert.datetime2milli(convert.string2datetime("02/01/2013 10:09:15", "%d/%m/%Y %H:%M:%S"))
            param.start_time_str = extract_bugzilla.milli2string(db, param.start_time)

            param.alias_file = self.settings.param.alias_file
            param.bug_list = wrap([813650])
            param.allow_private_bugs = self.settings.param.allow_private_bugs

            with es.threaded_queue(batch_size=1000) as output:
                etl(db, output, param, self.settings.alias, please_stop=None)

            Till(seconds=2).wait()  # MUST SLEEP WHILE ES DOES ITS INDEXING
            versions = get_all_bug_versions(es, 813650)

            flags = ["cf_status_firefox18", "cf_status_firefox19", "cf_status_firefox_esr17", "cf_status_b2g18"]
            for v in versions:
                if v.modified_ts>param.start_time:
                    for f in flags:
                        if v[f] != "fixed":
                            Log.error("813650 should have {{flag}}=='fixed'", {"flag": f})

    def test_whiteboard_screened(self):
        GOOD_BUG_TO_TEST=1046

        database.make_test_instance(self.settings.bugzilla)

        with MySQL(self.settings.bugzilla) as db:
            es = elasticsearch.make_test_instance("candidate", self.settings.candidate)

            #MARK BUG AS ONE OF THE SCREENED GROUPS
            database.add_bug_group(db, GOOD_BUG_TO_TEST, SCREENED_WHITEBOARD_BUG_GROUPS[0])
            db.flush()

            #SETUP RUN PARAMETERS
            param = Data()
            param.end_time = convert.datetime2milli(get_current_time(db))
            param.start_time = 0
            param.start_time_str = extract_bugzilla.milli2string(db, MIN_TIMESTAMP)

            param.alias_file = self.settings.param.alias_file
            param.bug_list = wrap([GOOD_BUG_TO_TEST]) # bug 1046 sees lots of whiteboard, and other field, changes
            param.allow_private_bugs = True

            with ThreadedQueue("etl queue", es, batch_size=1000) as output:
                etl(db, output, param, self.settings.alias, please_stop=None)

            Till(seconds=2).wait()  # MUST SLEEP WHILE ES DOES ITS INDEXING
            versions = get_all_bug_versions(es, GOOD_BUG_TO_TEST)

            for v in versions:
                if v.status_whiteboard not in (None, "", "[screened]"):
                    Log.error("Expecting whiteboard to be screened")

    def test_ambiguous_whiteboard_screened(self):
        GOOD_BUG_TO_TEST = 1046

        database.make_test_instance(self.settings.bugzilla)

        with MySQL(self.settings.bugzilla) as db:
            es = elasticsearch.make_test_instance("candidate", self.settings.candidate)

            #MARK BUG AS ONE OF THE SCREENED GROUPS
            database.add_bug_group(db, GOOD_BUG_TO_TEST, SCREENED_WHITEBOARD_BUG_GROUPS[0])
            #MARK BUG AS ONE OF THE *NOT* SCREENED GROUPS
            database.add_bug_group(db, GOOD_BUG_TO_TEST, "not screened")
            db.flush()

            #SETUP RUN PARAMETERS
            param = Data()
            param.end_time = convert.datetime2milli(get_current_time(db))
            param.start_time = 0
            param.start_time_str = extract_bugzilla.milli2string(db, MIN_TIMESTAMP)

            param.alias_file = self.settings.param.alias_file
            param.bug_list = wrap([GOOD_BUG_TO_TEST]) # bug 1046 sees lots of whiteboard, and other field, changes
            param.allow_private_bugs = True

            with ThreadedQueue("etl", es, batch_size=1000) as output:
                etl(db, output, param, self.settings.alias, please_stop=None)

            Till(seconds=2).wait()  # MUST SLEEP WHILE ES DOES ITS INDEXING
            versions = get_all_bug_versions(es, GOOD_BUG_TO_TEST)

            for v in versions:
                if v.status_whiteboard not in (None, "", "[screened]"):
                    Log.error("Expecting whiteboard to be screened")

    def test_incremental_has_correct_expires_on(self):
        # 813650, 726635 BOTH HAVE CHANGES IN 2013
        bugs = wrap([813650, 726635])
        start_incremental=convert.datetime2milli(convert.string2datetime("2013-01-01", "%Y-%m-%d"))

        es = elasticsearch.make_test_instance("candidate", self.settings.candidate)
        with MySQL(self.settings.bugzilla) as db:
            #SETUP FIRST RUN PARAMETERS
            param = Data()
            param.end_time = start_incremental
            param.start_time = MIN_TIMESTAMP
            param.start_time_str = extract_bugzilla.milli2string(db, param.start_time)

            param.alias_file = self.settings.param.alias_file
            param.bug_list = bugs
            param.allow_private_bugs = False

            with ThreadedQueue("etl queue", es, batch_size=1000) as output:
                etl(db, output, param, self.settings.alias, please_stop=None)

            #SETUP INCREMENTAL RUN PARAMETERS
            param = Data()
            param.end_time = convert.datetime2milli(datetime.utcnow())
            param.start_time = start_incremental
            param.start_time_str = extract_bugzilla.milli2string(db, param.start_time)

            param.alias_file = self.settings.param.alias_file
            param.bug_list = bugs
            param.allow_private_bugs = False

            with ThreadedQueue("etl queue", es, batch_size=1000) as output:
                etl(db, output, param, self.settings.alias, please_stop=None)

        for b in bugs:
            results = es.search({
                "query": {"filtered": {
                    "query": {"match_all": {}},
                    "filter": {"and":[
                        {"term":{"bug_id":b}},
                        {"range":{"expires_on":{"gte":convert.datetime2milli(datetime.utcnow())}}}
                    ]}
                }},
                "from": 0,
                "size": 200000,
                "sort": [],
                "fields": ["bug_id"]
            })

            if results.hits.total>1:
                Log.error("Expecting only one active bug_version record")


def verify_no_private_bugs(es, private_bugs):
    #VERIFY BUGS ARE NOT IN OUTPUT
    for b in private_bugs:
        versions = compare_es.get_all_bug_versions(es, b)

        if versions:
            Log.error("Expecting no version for private bug {{bug_id}}", {
                "bug_id": b
            })


def verify_public_bugs(es, private_bugs):
    #VERIFY BUGS ARE IN OUTPUT
    for b in private_bugs:
        versions = compare_es.get_all_bug_versions(es, b)
        if not versions:
            Log.error("Expecting versions for public bug {{bug_id}}", {
                "bug_id": b
            })


def verify_no_private_attachments(es, private_attachments):
    #VERIFY ATTACHMENTS ARE NOT IN OUTPUT
    for b in jx.select(private_attachments, "bug_id"):
        versions = compare_es.get_all_bug_versions(es, b)
        #WE ASSUME THE ATTACHMENT, IF IT EXISTS, WILL BE SOMEWHERE IN THE BUG IT
        #BELONGS TO, IF AT ALL
        for v in versions:
            for a in v.attachments:
                if a.attach_id in jx.select(private_attachments, "attach_id"):
                    Log.error("Private attachment should not exist")


def verify_no_private_comments(es, private_comments):
    data = es.search({
        "query": {"filtered": {
            "query": {"match_all": {}},
            "filter": {"and": [
                {"terms": {"comment_id": private_comments}}
            ]}
        }},
        "from": 0,
        "size": 200000,
        "sort": []
    })

    if jx.select(data.hits.hits, "_source"):
        Log.error("Expecting no comments")


#COMPARE ALL BUGS
def compare_both(candidate, reference, settings, some_bugs):
    File(settings.param.errors).delete()
    try_dir = settings.param.errors + "/try/"
    ref_dir = settings.param.errors + "/ref/"

    with Timer("Comparing to reference"):
        found_errors = False
        for bug_id in some_bugs:
            try:
                versions = jx.sort(
                    get_all_bug_versions(candidate, bug_id, datetime.utcnow()),
                    "modified_ts"
                )
                # WE CAN NOT EXPECT candidate TO BE UP TO DATE BECAUSE IT IS USING AN OLD IMAGE
                if not versions:
                    max_time = convert.milli2datetime(settings.bugzilla.expires_on)
                else:
                    max_time = convert.milli2datetime(versions.last().modified_ts)

                pre_ref_versions = get_all_bug_versions(reference, bug_id, max_time)
                ref_versions = jx.sort(
                    # ADDED TO FIX OLD PRODUCTION BUG VERSIONS
                    [compare_es.old2new(x, settings.bugzilla.expires_on) for x in pre_ref_versions],
                    "modified_ts"
                )

                can = value2json(versions, pretty=True)
                ref = value2json(ref_versions, pretty=True)
                if can != ref:
                    found_errors = True
                    File(try_dir + text_type(bug_id) + ".txt").write(can)
                    File(ref_dir + text_type(bug_id) + ".txt").write(ref)
            except Exception as e:
                found_errors = True
                Log.warning("Problem ETL'ing bug {{bug_id}}", bug_id=bug_id, cause=e)

        if found_errors:
            Log.error("DIFFERENCES FOUND (Differences shown in {{path}})", {
                "path": [try_dir, ref_dir]}
            )

if __name__ == "__main__":
    unittest.main()
