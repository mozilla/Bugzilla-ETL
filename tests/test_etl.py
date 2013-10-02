################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################

import json
from datetime import datetime
from bzETL import bz_etl

from bzETL.bz_etl import etl
from bzETL.util import db
from bzETL.util.cnv import CNV
from bzETL.util.db import DB, SQL
from bzETL.util.logs import Log
from bzETL.util.elasticsearch import ElasticSearch
from bzETL.util.files import File
from bzETL.util.query import Q
from bzETL.util.randoms import Random
from bzETL.util.startup import startup
from bzETL.util.strings import json_scrub
from bzETL.util.struct import Struct, Null
from bzETL.util.timer import Timer

from util import compare_es, database, elasticsearch
from util.compare_es import get_all_bug_versions



def test_specific_bugs(settings):
    """
    USE A MYSQL DATABASE TO FILL AN ES INSTANCE WITH BUG VERSIONS
    COMPARE THOSE VERSION TO A REFERENCE ES
    USE Fake_ES() INSTANCES TO KEEP THIS TEST LOCAL
    """
    settings.param.allow_private_bugs = True
    database.make_test_instance(settings.bugzilla)

    with DB(settings.bugzilla) as db:
        candidate=elasticsearch.make_test_instance("candidate", settings.candidate)
        reference=elasticsearch.open_test_instance("reference", settings.private_reference)

        #SETUP RUN PARAMETERS
        param=Struct()
        param.end_time=CNV.datetime2milli(datetime.utcnow())
        param.start_time=0
        param.alias_file=settings.param.alias_file
        param.bug_list=SQL(settings.param.bugs)
        param.allow_private_bugs=settings.param.allow_private_bugs

        etl(db, candidate, param)

        #COMPARE ALL BUGS
        compare_both(candidate, reference, settings, settings.param.bugs)




def random_sample_of_bugs(settings):
    """
    I USE THIS TO FIND BUGS THAT CAUSE MY CODE PROBLEMS.  OF COURSE, IT ONLY WORKS
    WHEN I HAVE A REFERENCE TO COMPARE TO
    """
    NUM_TO_TEST=100
    MAX_BUG_ID=900000


    with DB(settings.bugzilla) as db:
        candidate=elasticsearch.make_test_instance("candidate", settings.candidate)
        reference=ElasticSearch(settings.private_reference)

        #GO FASTER BY STORING LOCAL FILE
        local_cache=File(settings.param.temp_dir+"/private_bugs.json")
        if local_cache.exists:
            private_bugs=set(CNV.JSON2object(local_cache.read()))
        else:
            with Timer("get private bugs"):
                private_bugs= compare_es.get_private_bugs(reference)
                local_cache.write(CNV.object2JSON(private_bugs))

        while True:
            some_bugs=[b for b in [Random.int(MAX_BUG_ID) for i in range(NUM_TO_TEST)] if b not in private_bugs]

            #SETUP RUN PARAMETERS
            param=Struct()
            param.end_time=CNV.datetime2milli(datetime.utcnow())
            param.start_time=0
            param.alias_file=settings.param.alias_file
#            param.bugs_filter=SQL("bug_id in {{bugs}}", {"bugs":db.quote_value(some_bugs)})

            try:
                etl(db, candidate, param)

                #COMPARE ALL BUGS
                found_errors=compare_both(candidate, reference, settings, some_bugs)
                if found_errors:
                    Log.note("Errors found")
                    break
                else:
                    pass
            except Exception, e:
                Log.warning("Total failure during compare of bugs {{bugs}}", {"bugs":some_bugs}, e)



def test_private_etl(settings):
    """
    ASSUME THE DATABASE OF BUGS IS SMALL AND PROCESS ALL USING bz_etl()
    REQUIRES A REAL ES INSTANCE TO POINT TO
    """
    settings.param.incremental=False
    settings.param.allow_private_bugs=True

    database.make_test_instance(settings.bugzilla)
    es=elasticsearch.make_test_instance("candidate", settings.candidate)
    es_comments=elasticsearch.make_test_instance("candidate_comments", settings.candidate)
    bz_etl.main(settings, es, es_comments)

    ref=elasticsearch.open_test_instance("reference", settings.private_reference)
    compare_both(es, ref, settings, settings.param.bugs)

def test_public_etl(settings):
    """
    ASSUME THE DATABASE OF BUGS IS SMALL AND PROCESS ALL USING bz_etl()
    REQUIRES A REAL ES INSTANCE TO POINT TO
    """
    settings.param.incremental=False
    settings.param.allow_private_bugs=Null

    database.make_test_instance(settings.bugzilla)
    es=elasticsearch.make_test_instance("candidate", settings.test_main)
    es_comments=elasticsearch.make_test_instance("candidate_comments", settings.test_comments)
    bz_etl.main(settings, es, es_comments)

    ref=elasticsearch.open_test_instance("reference", settings.public_reference)
    compare_both(es, ref, settings, settings.param.bugs)


def test_private_bugs_do_not_show(settings):
    settings.param.allow_private_bugs=False
    settings.param.incremental=False

    private_bugs=settings.param.bugs[:3]  # THREE BUGS
    database.make_test_instance(settings.bugzilla)

    #MARK SOME BUGS PRIVATE
    with DB(settings.bugzilla) as db:
        for b in private_bugs:
            database.add_bug_group(db, b, "super secret")


    es=elasticsearch.make_test_instance("candidate", settings.test_main)
    es_c=elasticsearch.make_test_instance("candidate_comments", settings.test_comments)
    bz_etl.main(settings, es, es_c)

    #VERIFY BUGS ARE NOT IN OUTPUT
    for b in private_bugs:
        versions=compare_es.get_all_bug_versions(es, b)
        if len(versions)>0:
            Log.error("Expecting no version for private bug {{bug_id}}", {
                "bug_id":b
            })


def test_recent_private_bugs_do_not_show(settings):
    settings.param.allow_private_bugs=False
    settings.param.incremental=False
    private_bugs=settings.param.bugs[3:6]  # THREE BUGS
    database.make_test_instance(settings.bugzilla)

    es=elasticsearch.make_test_instance("candidate", settings.test_main)
    es_c=elasticsearch.make_test_instance("candidate_comments", settings.test_comments)
    bz_etl.main(settings, es, es_c)

    #MARK SOME BUGS PRIVATE
    with DB(settings.bugzilla) as db:
        for b in private_bugs:
            database.add_bug_group(db, b, "super secret")

    settings.incremental=True
    bz_etl.main(settings, es)

    #VERIFY BUGS ARE NOT IN OUTPUT
    for b in private_bugs:
        versions=compare_es.get_all_bug_versions(es, b)
        if len(versions)>0:
            Log.error("Expecting no version for private bug {{bug_id}}", {
                "bug_id":b
            })


def test_private_attachments_do_not_show(settings):
    settings.param.allow_private_bugs=False
    database.make_test_instance(settings.bugzilla)

    #MARK SOME STUFF PRIVATE
    with DB(settings.bugzilla) as db:
        private_attachments=db.query("""
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
            database.mark_attachment_private(db, a.attach_id)


    es=elasticsearch.make_test_instance("candidate", settings.test_main)
    es_c=elasticsearch.make_test_instance("candidate_comments", settings.test_comments)
    bz_etl.main(settings, es, es_c)

    #VERIFY ATTACHMENTS ARE NOT IN OUTPUT
    for b in Q.select(private_attachments, "bug_id"):
        versions=compare_es.get_all_bug_versions(es, b)
        for v in versions:
            for a in v.attachments:
                if a.attach_id in private_attachments:
                    Log.error("Private attachment should not exist")


def test_private_comments_do_not_show(settings):
    settings.param.allow_private_bugs=False
    database.make_test_instance(settings.bugzilla)

    #MARK SOME COMMENTS PRIVATE
    with DB(settings.bugzilla) as db:
        private_comments=db.query("""
            SELECT
                bug_id,
                comment_id
            FROM
                long_desc
            ORDER BY
                mod(comment_id, 7),
                comment_id
            LIMIT
                5
        """)

        for c in private_comments:
            database.mark_comment_private(db, c.comment_id)


    es=elasticsearch.make_test_instance("candidate", settings.test_main)
    es_c=elasticsearch.make_test_instance("candidate_comments", settings.test_comments)
    bz_etl.main(settings, es, es_c)

    #VERIFY ATTACHMENTS ARE NOT IN OUTPUT
    for c in private_comments:
        data=es.search({
            "query":{"filtered":{
                "query":{"match_all":{}},
                "filter":{"and":[
                    {"term":{"comment_id":c.comment_id}}
                ]}
            }},
            "from":0,
            "size":200000,
            "sort":[]
        })

        if len(Q.select(data.hits.hits, "_source")) > 0:
            Log.error("Expecting no comments")




#COMPARE ALL BUGS
def compare_both(candidate, reference, settings, some_bugs):
    File(settings.param.errors).delete()
    try_dir=settings.param.errors + "/try/"
    ref_dir=settings.param.errors + "/ref/"

    with Timer("Comparing to reference"):
        found_errors=False
        for bug_id in some_bugs:
            try:
                versions = Q.sort(
                    get_all_bug_versions(candidate, bug_id, datetime.utcnow()),
                    "modified_ts")
                # WE CAN NOT EXPECT candidate TO BE UP TO DATE BECAUSE IT IS USING AN OLD IMAGE
                if len(versions)==0:
                    max_time = CNV.milli2datetime(settings.bugzilla.expires_on)
                else:
                    max_time = CNV.milli2datetime(versions[-1].modified_ts)

                ref_versions = \
                    Q.sort(
                        map(
                            lambda x: compare_es.old2new(x, settings.bugzilla.expires_on),
                            get_all_bug_versions(reference, bug_id, max_time)
                        ),
                        "modified_ts"
                    )

                can = json.dumps(json_scrub(versions), indent=4, sort_keys=True, separators=(',', ': '))
                ref = json.dumps(json_scrub(ref_versions), indent=4, sort_keys=True, separators=(',', ': '))
                if can != ref:
                    found_errors=True
                    File(try_dir + unicode(bug_id) + ".txt").write(can)
                    File(ref_dir + unicode(bug_id) + ".txt").write(ref)
            except Exception, e:
                found_errors=True
                Log.warning("Problem ETL'ing bug {{bug_id}}", {"bug_id":bug_id}, e)

        if found_errors:
            Log.error("DIFFERENCES FOUND (Differences shown in {{path}})", {
                "path":[try_dir, ref_dir]}
            )



def main():
    try:
        settings=startup.read_settings()
        Log.start(settings.debug)
#        test_specific_bugs(settings)
#         test_private_etl(settings)
        test_public_etl(settings)
        Log.note("All tests pass!  Success!!")
    finally:
        Log.stop()

main()

