from datetime import datetime
import functools
from types import MethodType
import unittest
from pymysql.times import TimeDelta
from bzETL.extract_bugzilla import SCREENED_WHITEBOARD_BUG_GROUPS
from pyLibrary.env import startup, elasticsearch
from pyLibrary import struct
from pyLibrary import convert
from pyLibrary.env.emailer import Emailer
from pyLibrary.env.logs import Log, extract_stack
from pyLibrary.maths import Math
from pyLibrary.queries import qb
from pyLibrary.struct import coalesce, Dict

# WRAP Log.error TO SHOW THE SPECIFIC ERROR IN THE LOGFILE
if not hasattr(Log, "old_error"):
    Log.old_error = Log.error
    def new_error(cls, *args):
        try:
            Log.old_error(*args, stack_depth=1)
        except Exception, e:
            Log.warning("testing error", e, stack_depth=1)
            raise e

    ##ASSIGN AS CLASS METHOD
    Log.error=MethodType(new_error, Log)

NOW = convert.datetime2milli(datetime.utcnow())
A_WHILE_AGO = int(NOW - TimeDelta(minutes=10).total_seconds()*1000)


class TestLookForLeaks(unittest.TestCase):
    def setUp(self):
        test_name = self._testMethodName
        settings = startup.read_settings(filename="leak_check_settings.json")
        Log.start(settings.debug)
        Log.note("\nStart {{test_name}}", locals())
        self.private = elasticsearch.Index(settings.private)
        self.public = elasticsearch.Index(settings.public)
        self.public_comments = elasticsearch.Index(settings.public_comments)
        self.settings = settings

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

        return reversed(list(qb.intervals(0, max_bug_id, self.settings.param.increment)))

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

                Log.note("{{num}} leaks!! {{bugs}}", {
                    "num": len(leaked_bugs),
                    "bugs": qb.run({
                        "from":leaked_bugs,
                        "select":["bug_id", "bug_version_num", {"name":"modified_ts", "value":lambda d: convert.datetime2string(convert.milli2datetime(d.modified_ts))}],
                        "sort":"bug_id"
                    })
                })
                for b in leaked_bugs:
                    Log.note("{{bug_id}} has bug groups {{bug_group}}\n{{version|indent}}", {
                        "bug_id": b.bug_id,
                        "bug_group": private_ids[b.bug_id],
                        "version": milli2datetime(b)
                    })

            #CHECK FOR LEAKED COMMENTS, BEYOND THE ONES LEAKED BY BUG
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

                Log.warning("{{num}} comments marked private have leaked!\n{{comments|indent}}", {
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

            private_attachments = qb.run({
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
                private_attachments = qb.run({
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
            fields=["bug_id", "product", "component", "status_whiteboard", "bug_group", "modified_ts"],
            limit=100
        )

        if leaked_whiteboard:
            for l in leaked_whiteboard:
                l.modified_ts=convert.datetime2string(convert.milli2datetime(l.modified_ts))

            Log.error("Whiteboard leaking:\n{{leak|indent}}", {"leak": leaked_whiteboard})


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
        return qb.select(results.hits.hits, "fields")
    else:
        results = es.search(query)
        return qb.select(results.hits.hits, "_source")




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
            if convert.value2number(r) > 800000000000:
                return convert.datetime2string(convert.milli2datetime(r), "%Y-%m-%d %H:%M:%S")
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
                return qb.sort(output)
            except Exception:
                return output
        else:
            return r
    except Exception, e:
        Log.warning("Can not scrub: {{json}}", {"json": r}, e)



def main():
    try:
        suite = unittest.TestSuite()
        suite.addTest(unittest.defaultTestLoader.loadTestsFromName("leak_check"))
        results = unittest.TextTestRunner(failfast=False).run(suite)

        if results.errors or results.failures:
            error(results)
    except Exception, e:
        error(Dict(errors=[e]))
    finally:
        pass


def error(results):
    settings = startup.read_settings()

    content = []
    for e in results.errors:
        content.append("ERROR: "+str(e[0]._testMethodName))
    for f in results.failures:
        content.append("FAIL:  "+str(f[0]._testMethodName))

    Emailer(settings.email).send_email(
        text_data = "\n".join(content)
    )


if __name__=="__main__":
    main()
