from datetime import datetime
import unittest
from pymysql.times import TimeDelta
from bzETL.extract_bugzilla import SCREENED_WHITEBOARD_BUG_GROUPS
from bzETL.util.env import startup
from bzETL.util import struct
from bzETL.util.cnv import CNV
from bzETL.util.env.elasticsearch import ElasticSearch
from bzETL.util.env.emailer import Emailer
from bzETL.util.env.logs import Log
from bzETL.util.maths import Math
from bzETL.util.queries import Q
from bzETL.util.struct import nvl


NOW = CNV.datetime2milli(datetime.utcnow())
A_WHILE_AGO = int(NOW - TimeDelta(minutes=10).total_seconds()*1000)


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

        return reversed(list(Q.intervals(0, max_bug_id, self.settings.param.increment)))

    def test_private_bugs_not_leaking(self):
        # FOR ALL BUG BLOCKS
        for min_id, max_id in self.blocks_of_bugs():
            results = get(
                self.private,
                {"and": [
                    {"match_all": {}},
                    {"and": [
                        {"range": {"bug_id": {"gte": min_id, "lt": max_id}}},
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
                if self.settings.delete:
                    self.public.delete_record(
                        {"terms":{"bug_id":leaked_bugs.bug_id}}
                    )

                Log.note("{{num}} leaks!! {{bugs}}", {
                    "num": len(leaked_bugs),
                    "bugs": Q.run({
                        "from":leaked_bugs,
                        "select":["bug_id", "bug_version_num", {"name":"modified_ts", "value":lambda d: CNV.datetime2string(CNV.milli2datetime(d.modified_ts))}],
                        "sort":"bug_id"
                    })
                })
                for b in leaked_bugs:
                    Log.note("{{bug_id}} has bug groups {{bug_group}}\n{{version|indent}}", {
                        "bug_id": b.bug_id,
                        "bug_group": private_ids[b.bug_id],
                        "version": milli2datetime(b)
                    })
                Log.error("Bugs have leaked!")

            #CHECK FOR LEAKED COMMENTS, BEYOND THE ONES LEAKED BY BUG
            leaked_comments = get(
                self.public_comments,
                {"terms": {"bug_id": private_ids.keys()}},
                limit=20
            )
            if leaked_comments:
                if self.settings.delete:
                    self.public_comments.delete_record(
                        {"terms":{"bug_id":leaked_comments.bug_id}}
                    )

                Log.error("{{num}} comments marked private have leaked!\n{{comments|indent}}", {
                    "num": len(leaked_comments),
                    "comments": leaked_comments
                })


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

            private_attachments = Q.run({
                "from": bugs_w_private_attachments,
                "select": "attachments.attach_id",
                "where": {"or": [
                    {"exists": "bug_group"},
                    {"terms": {"attachments.isprivate": ['1', True, 1]}}
                ]}
            })
            try:
                private_attachments = [int(v) for v in private_attachments]
            except Exception, e:
                private_attachments = Q.run({
                    "from": bugs_w_private_attachments,
                    "select": "attachments.attach_id",
                    "where": {"or": [
                        {"exists": "bug_group"},
                        {"terms": {"attachments.isprivate": ['1', True, 1]}}
                    ]}
                })

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
                # fields=["bug_id", "attachments"]
            )

            #

            if leaked_bugs:
                if self.settings.delete:
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
            if self.settings.delete:
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
            ]},
            fields=["bug_id", "product", "component", "status_whiteboard", "bug_group", "modified_ts"],
            limit=100

        )

        if leaked_whiteboard:
            for l in leaked_whiteboard:
                l.modified_ts=CNV.datetime2string(CNV.milli2datetime(l.modified_ts))

            Log.error("Whiteboard leaking:\b{{leak}}", {"leak": leaked_whiteboard})


def get(es, esfilter, fields=None, limit=None):
    query = struct.wrap({
        "query": {"filtered": {
            "query": {"match_all": {}},
            "filter": esfilter
        }},
        "from": 0,
        "size": nvl(limit, 200000),
        "sort": []
    })

    if fields:
        query.fields=fields
        results = es.search(query)
        return Q.select(results.hits.hits, "fields")
    else:
        results = es.search(query)
        return Q.select(results.hits.hits, "_source")




def milli2datetime(r):
    """
    CONVERT ANY longs INTO TIME STRINGS
    """
    try:
        if r == None:
            return None
        elif isinstance(r, basestring):
            return r
        elif Math.is_number(r):
            #                       1382068456000
            if CNV.value2number(r) > 800000000000:
                return CNV.datetime2string(CNV.milli2datetime(r), "%Y-%m-%d %H:%M:%S")
            else:
                return r
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



def main():
    try:
        suite = unittest.TestSuite()
        suite.addTest(unittest.defaultTestLoader.loadTestsFromName("leak_check"))
        results = unittest.TextTestRunner(failfast=False).run(suite)

        if results.errors or results.failures:
            error(results)
    except Exception, e:
        error()
    finally:
        pass

def error(results):
    settings = startup.read_settings()

    content = []
    for e in results.errors:
        content.append("FAIL: "+str(e[0]._testMethodName))
    for f in results.failures:
        content.append("FAIL:  "+str(f[0]._testMethodName))


    Emailer(settings.email).send_email(
        text_data = "\n".join(content)
    )


if __name__=="__main__":
    main()
