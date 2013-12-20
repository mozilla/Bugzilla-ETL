from datetime import datetime, timedelta
import unittest
from bzETL.extract_bugzilla import SCREENED_WHITEBOARD_BUG_GROUPS
from bzETL.util import startup, struct
from bzETL.util.cnv import CNV
from bzETL.util.elasticsearch import ElasticSearch
from bzETL.util.logs import Log
from bzETL.util.maths import Math
from bzETL.util.queries import Q
from util import elasticsearch


class TestLookForLeaks(unittest.TestCase):
    def setUp(self):
        settings = startup.read_settings(filename="leak_check_settings.json")
        Log.start(settings.debug)
        self.private = ElasticSearch(settings.private)
        self.public = ElasticSearch(settings.public)
        self.public_comments = ElasticSearch(settings.public_comments)
        self.settings = settings

    def tearDown(self):
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

        return reversed(list(Q.range(0, max_bug_id, self.settings.param.increment)))

    def test_private_bugs_not_leaking(self):
        # FOR ALL BUG BLOCKS
        for min_id, max_id in self.blocks_of_bugs():
            results = elasticsearch.get(
                self.private,
                {"and": [
                    {"match_all": {}},
                    {"and": [
                        {"range": {"bug_id": {"gte": min_id, "lt": max_id}}},
                        {"exists": {"field": "bug_group"}},
                        {"range": {"expires_on": {"gte": CNV.datetime2milli(datetime.utcnow())}}}
                    ]}
                ]},
                ["bug_id", "bug_group"]
            )

            private_ids = {b.bug_id: b.bug_group for b in results}

            Log.note("Ensure {{num}} bugs did not leak", {
                "num": len(private_ids.keys())
            })

            # VERIFY NONE IN PUBLIC
            leaked_bugs = elasticsearch.get(
                self.public,
                {"and": [
                    {"terms": {"bug_id": private_ids.keys()}},
                    {"range": {"modified_ts": {"lt": datetime.utcnow() - timedelta(minutes=10)}}} # SOME BUGS WILL LEAK FOR A LITTLE WHILE
                ]}
            )

            if leaked_bugs:
                Log.note("{{num}} leaks!! {{bugs}}", {
                    "num": len(leaked_bugs),
                    "bugs":CNV.object2JSON(set(Q.sort(Q.select(leaked_bugs, "bug_id"))))
                })
                for b in leaked_bugs:
                    Log.note("{{bug_id}} has bug groups {{bug_group}}\n{{version|indent}}", {
                        "bug_id": b.bug_id,
                        "bug_group": private_ids[b.bug_id],
                        "version": milli2datetime(b)
                    })
                Log.error("Bugs have leaked!")


    def test_private_attachments_not_leaking(self):
        for min_id, max_id in self.blocks_of_bugs():
            # FIND ALL PRIVATE ATTACHMENTS
            bugs_w_private_attachments = elasticsearch.get(
                self.private,
                {"and": [
                    {"range": {"bug_id": {"gte": min_id, "lt": max_id}}},
                    {"nested": {
                        "path": "attachments",
                        "query": {"filtered": {
                            "query": {"match_all": {}},
                            "filter": {"term": {"attachments.isprivate": 1}}
                        }}
                    }}
                ]},
                fields=["bug_id", "attachments"]
            )

            private_attachments = Q.run({
                "from":bugs_w_private_attachments,
                "select":"attachments.attach_id",
                "where":{"or":[
                    {"exists": "bug_group"},
                    {"terms": {"attachments.attachments\.isprivate": ['1', True, 1]}}
                ]}
            })

            #VERIFY NONE IN PUBLIC
            leaked_bugs = elasticsearch.get(
                self.public,
                {"and": [
                    {"range": {"bug_id": {"gte": min_id, "lt": max_id}}},
                    {"range": {"modified_ts": {"lt": datetime.utcnow() - timedelta(minutes=10)}}}, # SOME BUGS WILL LEAK FOR A LITTLE WHILE
                    {"nested": {
                        "path": "attachments",
                        "query": {"filtered": {
                            "query": {"match_all": {}},
                            "filter": {"terms": {"attach_id": private_attachments}}
                        }}
                    }}
                ]}
                # fields=["bug_id", "attachments"]
            )

            if leaked_bugs:
                Log.note("{{num}} bugs with private attachments have leaked!", {"num":len(leaked_bugs)})
                for b in leaked_bugs:
                    Log.note("{{bug_id}} has private_attachment\n{{version|indent}}", {
                        "bug_id": b.bug_id,
                        "version": b
                    })
                Log.error("Attachments have leaked!")


    def test_private_comments_not_leaking(self):
        leaked_comments = elasticsearch.get(
            self.public_comments,
            {"term":{"isprivate":"1"}},
            limit=20
        )
        if leaked_comments:
            Log.error("{{num}} comments marked private have leaked!\n{{comments|indent}}", {
                "num":len(leaked_comments),
                "comments":leaked_comments
            })


    def test_confidential_whiteboard_is_screened(self):
        leaked_whiteboard = elasticsearch.get(
            self.private,
            {"and": [
                {"terms": {"bug_group": SCREENED_WHITEBOARD_BUG_GROUPS}},
                {"not": {"terms": {"status_whiteboard": ["", "[screened]"]}}},
                {"range": {"expires_on": {"gte": CNV.datetime2milli(datetime.utcnow())}}}
            ]},
            fields=["bug_id", "product", "component", "status_whiteboard", "bug_group"],
            limit=100

        )

        if leaked_whiteboard:
            Log.error("Whiteboard leaking:\b{{leak}}", {"leak": leaked_whiteboard})


def milli2datetime(r):
    try:
        if r == None:
            return None
        elif isinstance(r, basestring):
            return r
        elif Math.is_number(r):
            if CNV.value2number(r)>800000000000:
               return CNV.datetime2string(CNV.milli2datetime(r), "%Y-%m-%d %H:%M:%S")
        elif isinstance(r, dict):
            output = {}
            for k, v in r.items():
                v = milli2datetime(v)
                if v != None:
                    output[k.lower()] = v
            return output
        elif hasattr(r, '__iter__'):
            output = []
            for v in r:
                v = milli2datetime(v)
                if v != None:
                    output.append(v)
            if not output:
                return None
            try:
                return Q.sort(output)
            except Exception:
                return output
        else:
            return r
    except Exception, e:
        Log.warning("Can not scrub: {{json}}", {"json": r})





