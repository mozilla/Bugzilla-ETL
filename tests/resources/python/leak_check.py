# encoding: utf-8

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import unittest

from bzETL.extract_bugzilla import SCREENED_WHITEBOARD_BUG_GROUPS
from pyLibrary import convert
from pyLibrary.debugs import startup, constants
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import coalesce, Dict, set_default, listwrap
from pyLibrary.dot import wrap
from pyLibrary.env import elasticsearch
from pyLibrary.env.emailer import Emailer
from pyLibrary.queries import jx

# WRAP Log.error TO SHOW THE SPECIFIC ERROR IN THE LOGFILE
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import MINUTE, Duration

# if not hasattr(Log, "old_error"):
#     Log.old_error = Log.error
#     def new_error(cls, *args):
#         try:
#             Log.old_error(*args, stack_depth=1)
#         except Exception, e:
#             Log.warning("testing error", e, stack_depth=1)
#             raise e
#
#     ##ASSIGN AS CLASS METHOD
#     Log.error=MethodType(new_error, Log)

SETTINGS = Dict()
_NOW = Date.now()
NOW = _NOW.milli
A_WHILE_AGO = (_NOW - MINUTE * 10).milli


class TestLookForLeaks(unittest.TestCase):
    def setUp(self):
        set_default(SETTINGS, startup.read_settings())
        constants.set(SETTINGS.constants)

        test_name = self._testMethodName
        Log.note("\nStart {{test_name}}", locals())
        self.private = elasticsearch.Index(SETTINGS.private)
        self.public = elasticsearch.Index(SETTINGS.public)
        self.public_comments = elasticsearch.Index(SETTINGS.public_comments)
        self.settings = SETTINGS

    def tearDown(self):
        test_name = self._testMethodName
        Log.note("Done {{test_name}}\n", locals())
        Log.stop()

    def blocks_of_bugs(self):
        max_bug_id = self.private.search({
            "query": {"filtered": {
                "query": {"match_all": {}},
                "filter": {"and": [{"match_all": {}}]}
            }},
            "from": 0,
            "size": 0,
            "sort": [],
            "facets": {"0": {"statistical": {"field": "bug_id"}}}
        }).facets["0"].max

        return reversed(list(jx.intervals(0, max_bug_id, self.settings.param.increment)))

    def test_private_bugs_not_leaking(self):
        bad_news = False

        # FOR ALL BUG BLOCKS
        for min_id, max_id in self.blocks_of_bugs():
            results = get(
                self.private,
                {"and": [
                    {"match_all": {}},
                    {"and": [
                        {"range": {"bug_id": {"gte": min_id, "lt": max_id}}},
                        {"not": {"terms": {"bug_id": [0] + listwrap(SETTINGS.param.ignore_bugs)}}},
                        {"exists": {"field": "bug_group"}},
                        {"range": {"expires_on": {"gte": NOW}}},  #CURRENT RECORDS
                        {"range": {"modified_ts": {"lt": A_WHILE_AGO}}}, #OF A MINIMUM AGE
                    ]}
                ]},
                ["bug_id", "bug_group", "modified_ts"]
            )

            private_ids = {b.bug_id: b.bug_group for b in results}

            Log.note("Ensure {{num}} bugs did not leak", {
                "num": len(private_ids.keys())
            })

            # VERIFY NONE IN PUBLIC
            leaked_bugs = get(
                self.public,
                {"and": [
                    {"terms": {"bug_id": private_ids.keys()}},
                    {"range": {"expires_on": {"gte": NOW}}} # SOME BUGS WILL LEAK FOR A LITTLE WHILE
                ]}
            )

            if leaked_bugs:
                bad_news = True
                if self.settings.param.delete:
                    self.public.delete_record(
                        {"terms":{"bug_id":leaked_bugs.bug_id}}
                    )

                Log.note("{{num}} leaks!! {{bugs|json}}", {
                    "num": len(leaked_bugs),
                    "bugs": jx.run({
                        "from":leaked_bugs,
                        "select":[
                            "bug_id",
                            "bug_version_num",
                            {"name": "expires_on", "value":lambda d: Date(d.expires_on/1000).format()},
                            {"name":"modified_ts", "value":lambda d: Date(d.modified_ts/1000).format()}
                        ],
                        "sort":"bug_id"
                    })
                })

            #CHECK FOR LEAKED COMMENTS
            leaked_comments = get(
                self.public_comments,
                {"terms": {"bug_id": private_ids.keys()}},
                limit=20
            )
            if leaked_comments:
                bad_news = True

                if self.settings.param.delete:
                    self.public_comments.delete_record(
                        {"terms":{"bug_id":leaked_comments.bug_id}}
                    )

                Log.warning("{{num}} comments marked private have leaked!\n{{comments|json|indent}}", {
                    "num": len(leaked_comments),
                    "comments": leaked_comments
                })

        if bad_news:
            Log.error("Bugs have leaked!")


    def test_private_attachments_not_leaking(self):
        for min_id, max_id in self.blocks_of_bugs():
            # FIND ALL PRIVATE ATTACHMENTS
            bugs_w_private_attachments = get(
                self.private,
                {"and": [
                    {"range": {"bug_id": {"gte": min_id, "lt": max_id}}},
                    {"range": {"expires_on": {"gte": NOW}}},  #CURRENT RECORDS
                    {"range": {"modified_ts": {"lt": A_WHILE_AGO}}}, #OF A MINIMUM AGE
                    {"nested": { #HAS ATTACHMENT.
                        "path": "attachments",
                        "query": {"filtered": {
                            "query": {"match_all": {}},
                            "filter": {"exists": {"field":"attachments.attach_id"}}
                        }}
                    }},
                    {"or":[
                        {"nested": { #PRIVATE ATTACHMENT, OR...
                            "path": "attachments",
                            "query": {"filtered": {
                                "query": {"match_all": {}},
                                "filter": {"term": {"attachments.isprivate": 1}}
                            }}
                        }},
                        {"exists":{"field":"bug_group"}}  # ...PRIVATE BUG
                    ]}
                ]},
                fields=["bug_id", "bug_group", "attachments", "modified_ts"]
            )

            attachments = []
            for b in bugs_w_private_attachments:
                for a in b.attachments:
                    bb = b.copy()
                    bb.attachments=a
                    attachments.append(bb)

            private_attachments = jx.run({
                "from": attachments,
                "select": "attachments.attach_id",
                "where": {"or": [
                    {"exists": "bug_group"},
                    {"terms": {"attachments.isprivate": ['1', True, 1]}}
                ]}
            })
            private_attachments = [int(v) for v in private_attachments]

            Log.note("Ensure {{num}} attachments did not leak", {
                "num": len(private_attachments)
            })

            #VERIFY NONE IN PUBLIC
            leaked_bugs = get(
                self.public,
                {"and": [
                    {"range": {"bug_id": {"gte": min_id, "lt": max_id}}},
                    {"range": {"expires_on": {"gte": NOW}}}, # CURRENT BUGS
                    {"nested": {
                        "path": "attachments",
                        "query": {"filtered": {
                            "query": {"match_all": {}},
                            "filter": {"terms": {"attach_id": private_attachments}}
                        }}
                    }}
                ]}
            )

            if leaked_bugs:
                if self.settings.param.delete:
                    self.public.delete_record(
                        {"terms":{"bug_id":leaked_bugs.bug_id}}
                    )

                Log.note("{{num}} bugs with private attachments have leaked!", {"num": len(leaked_bugs)})
                for b in leaked_bugs:
                    Log.note("{{bug_id}} has private_attachment\n{{version|indent}}", {
                        "bug_id": b.bug_id,
                        "version": b
                    })
                Log.error("Attachments have leaked!")


    def test_private_comments_not_leaking(self):
        leaked_comments = get(
            self.public_comments,
            {"term": {"isprivate": "1"}},
            limit=20
        )
        if leaked_comments:
            if self.settings.param.delete:
                self.public_comments.delete_record(
                    {"terms":{"bug_id":leaked_comments.bug_id}}
                )

            Log.error("{{num}} comments marked private have leaked!\n{{comments|indent}}", {
                "num": len(leaked_comments),
                "comments": leaked_comments
            })


    def test_confidential_whiteboard_is_screened(self):
        leaked_whiteboard = get(
            self.private,
            {"and": [
                {"terms": {"bug_group": SCREENED_WHITEBOARD_BUG_GROUPS}},
                {"exists": {"field": "status_whiteboard"}},
                {"not": {"terms": {"status_whiteboard": ["", "[screened]"]}}},
                {"range": {"expires_on": {"gte": NOW}}}, #CURRENT RECORDS
                {"range": {"modified_ts": {"lt": A_WHILE_AGO}}}, #OF A MINIMUM AGE
                {"not":{"terms":{"bug_id": self.settings.param.ignore_bugs}}} if self.settings.param.ignore_bugs else {"match_all":{}}
            ]},
            fields=["bug_id", "product", "component", "status_whiteboard", "bug_group", "modified_ts", "expires_on"],
            limit=100
        )

        if leaked_whiteboard:
            for l in leaked_whiteboard:
                l.modified_ts=Date(l.modified_ts/1000).format()

            Log.note("Whiteboard leaking:\n{{leak|indent}}", leak=leaked_whiteboard)
            Log.error("Whiteboard leaking")

    def test_etl_still_working(self):
        query = {
        	"query":{"filtered":{
        		"query":{"match_all":{}},
        		"filter":{"and":[
        			{"match_all":{}},
        			{"range":{"modified_ts":{"gte":1475280000000}}}
        		]}
        	}},
        	"from":0,
        	"size":0,
        	"sort":[],
        	"facets":{"0":{"statistical":{"field":"modified_ts"}}}
        }

        result = self.public.search(query)
        max_timestamp = Date(result.facets["0"].max / 1000)
        if max_timestamp < Date.now() - Duration("2hour"):
            Log.error("Public ETL is behind")

        result = self.private.search(query)
        max_timestamp = Date(result.facets["0"].max / 1000)
        if max_timestamp < Date.now() - Duration("2hour"):
            Log.error("Private ETL is behind")


def get(es, esfilter, fields=None, limit=None):
    query = wrap({
        "query": {"filtered": {
            "query": {"match_all": {}},
            "filter": esfilter
        }},
        "from": 0,
        "size": coalesce(limit, 200000),
        "sort": []
    })

    if fields:
        query.fields=fields
        results = es.search(query)
        return jx.select(results.hits.hits, "fields")
    else:
        results = es.search(query)
        return jx.select(results.hits.hits, "_source")


def main():
    try:
        set_default(SETTINGS, startup.read_settings())
        constants.set(SETTINGS.constants)

        suite = unittest.TestSuite()
        suite.addTest(unittest.defaultTestLoader.loadTestsFromName("leak_check"))
        results = unittest.TextTestRunner(failfast=False).run(suite)

        if results.errors or results.failures:
            error(results)
    except Exception, e:
        Log.error("Problem", cause=e)


def error(results):
    content = []
    for e in results.errors:
        content.append("ERROR: "+unicode(e[0]._testMethodName))
    for f in results.failures:
        content.append("FAIL:  "+unicode(f[0]._testMethodName))

    Emailer(SETTINGS.email).send_email(
        text_data = "\n".join(content)
    )


if __name__=="__main__":
    main()
