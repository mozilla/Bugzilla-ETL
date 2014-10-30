# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

# REPLACES THE KETTLE FLOW CONTROL PROGRAM, AND BASH SCRIPT



from pyLibrary.maths import Math
from pyLibrary import struct, jsons
from pyLibrary.env.logs import Log
from pyLibrary.struct import Struct, nvl
from pyLibrary.env.files import File
from pyLibrary.env import startup
from pyLibrary.thread.threads import Queue, Thread, AllThread, Lock, ThreadedQueue
from pyLibrary.cnv import CNV
from pyLibrary.env.elasticsearch import ElasticSearch
from pyLibrary.queries import Q
from pyLibrary.sql.db import DB

from bzETL import parse_bug_history, transform_bugzilla, extract_bugzilla, alias_analysis
from pyLibrary.times.timer import Timer
from extract_bugzilla import get_private_bugs_for_delete, get_recent_private_attachments, get_recent_private_comments, get_comments, get_comments_by_id, get_recent_private_bugs, get_current_time, get_bugs, get_dependencies, get_flags, get_new_activities, get_bug_see_also, get_attachments, get_tracking_flags, get_keywords, get_cc, get_bug_groups, get_duplicates
from parse_bug_history import BugHistoryParser


db_cache_lock = Lock()
db_cache = []
comment_db_cache_lock = Lock()
comment_db_cache = []

#HERE ARE ALL THE FUNCTIONS WE WANT TO RUN, IN PARALLEL
get_stuff_from_bugzilla = [
    get_bugs,
    get_dependencies,
    get_flags,
    get_new_activities,
    get_bug_see_also,
    get_attachments,
    get_tracking_flags,
    get_keywords,
    get_cc,
    get_bug_groups,
    get_duplicates
]


def etl_comments(db, es, param, please_stop):
    # CONNECTIONS ARE EXPENSIVE, CACHE HERE
    with comment_db_cache_lock:
        if not comment_db_cache:
            comment_db = DB(db)
            comment_db_cache.append(comment_db)

    with comment_db_cache_lock:
        Log.note("Read comments from database")
        comments = get_comments(comment_db_cache[0], param)

    for g, c in Q.groupby(comments, size=500):
        with Timer("Write {{num}} comments to ElasticSearch", {"num": len(c)}):
            es.extend({"id": cc.comment_id, "value": cc} for cc in c)


def etl(db, output_queue, param, please_stop):
    """
    PROCESS RANGE, AS SPECIFIED IN param AND PUSH
    BUG VERSION RECORDS TO output_queue
    """

    # CONNECTIONS ARE EXPENSIVE, CACHE HERE
    with db_cache_lock:
        if not db_cache:
            with Timer("open connections to db"):
                for f in get_stuff_from_bugzilla:
                    db = DB(db)
                    db_cache.append(db)

    db_results = Queue(max=2**30)
    with db_cache_lock:
        # ASYMMETRIC MULTI THREADING TO GET RECORDS FROM DB
        with AllThread() as all:
            for i, f in enumerate(get_stuff_from_bugzilla):
                def process(target, db, param, please_stop):
                    db_results.extend(target(db, param))

                all.add(process, f, db_cache[i], param.copy())
    db_results.add(Thread.STOP)

    sorted = Q.sort(db_results, [
        "bug_id",
        "_merge_order",
        {"field": "modified_ts", "sort": -1},
        "modified_by"
    ])

    process = BugHistoryParser(param, output_queue)
    for s in sorted:
        process.processRow(s)
    process.processRow(struct.wrap({"bug_id": parse_bug_history.STOP_BUG, "_merge_order": 1}))


def run_both_etl(db, output_queue, es_comments, param):
    comment_thread = Thread.run("etl comments", etl_comments, db, es_comments, param)
    process_thread = Thread.run("etl", etl, db, output_queue, param)

    result = comment_thread.join()
    if result.exception:
        Log.error("etl_comments had problems", result.exception)

    result = process_thread.join()
    if result.exception:
        Log.error("etl had problems", result.exception)


def setup_es(settings, db, es, es_comments):
    """
    SETUP ES CONNECTIONS TO REFLECT IF WE ARE RESUMING, INCREMENTAL, OR STARTING OVER
    """
    current_run_time = get_current_time(db)

    if File(settings.param.first_run_time).exists and File(settings.param.last_run_time).exists:
        # INCREMENTAL UPDATE; DO NOT MAKE NEW INDEX
        last_run_time = long(File(settings.param.last_run_time).read())
        if not es:
            es = ElasticSearch(settings.es)
            es_comments = ElasticSearch(settings.es_comments)
    elif File(settings.param.first_run_time).exists:
        # DO NOT MAKE NEW INDEX, CONTINUE INITIAL FILL
        try:
            last_run_time = 0
            current_run_time = long(File(settings.param.first_run_time).read())
            if not es:
                if not settings.es.alias:
                    temp = ElasticSearch(settings.es).get_proto(settings.es.index)
                    settings.es.alias = settings.es.index
                    settings.es.index = temp.last()
                es = ElasticSearch(settings.es)
                es.set_refresh_interval(1)  #REQUIRED SO WE CAN SEE WHAT BUGS HAVE BEEN LOADED ALREADY

                if not settings.es_comments.alias:
                    temp = ElasticSearch(settings.es_comments).get_proto(settings.es_comments.index)
                    settings.es_comments.alias = settings.es_comments.index
                    settings.es_comments.index = temp.last()
                es_comments = ElasticSearch(settings.es_comments)
        except Exception, e:
            Log.warning("can not resume ETL, restarting", e)
            File(settings.param.first_run_time).delete()
            return setup_es(settings, db, es, es_comments)
    else:
        # START ETL FROM BEGINNING, MAKE NEW INDEX
        last_run_time = 0
        if not es:
            # BUG VERSIONS
            schema = File(settings.es.schema_file).read()
            if transform_bugzilla.USE_ATTACHMENTS_DOT:
                schema = schema.replace("attachments_", "attachments\\.")
            schema=CNV.JSON2object(schema, paths=True)
            schema.settings=jsons.expand_dot(schema.settings)
            if not settings.es.alias:
                settings.es.alias = settings.es.index
                settings.es.index = ElasticSearch.proto_name(settings.es.alias)
            es = ElasticSearch.create_index(settings.es, schema, limit_replicas=True)

            # BUG COMMENTS
            comment_schema = File(settings.es_comments.schema_file).read()
            comment_schema=CNV.JSON2object(comment_schema, paths=True)
            comment_schema.settings=jsons.expand_dot(comment_schema.settings)
            if not settings.es_comments.alias:
                settings.es_comments.alias = settings.es_comments.index
                settings.es_comments.index = ElasticSearch.proto_name(settings.es_comments.alias)
            es_comments = ElasticSearch.create_index(settings.es_comments, comment_schema, limit_replicas=True)

        File(settings.param.first_run_time).write(unicode(CNV.datetime2milli(current_run_time)))

    return current_run_time, es, es_comments, last_run_time


def incremental_etl(settings, param, db, es, es_comments, output_queue):
    ####################################################################
    ## ES TAKES TIME TO DELETE RECORDS, DO DELETE FIRST WITH HOPE THE
    ## INDEX GETS A REWRITE DURING ADD OF NEW RECORDS
    ####################################################################

    #REMOVE PRIVATE BUGS
    private_bugs = get_private_bugs_for_delete(db, param)
    Log.note("Ensure the following private bugs are deleted:\n{{private_bugs|indent}}", {"private_bugs": sorted(private_bugs)})
    for g, delete_bugs in Q.groupby(private_bugs, size=1000):
        still_existing = get_bug_ids(es, {"terms": {"bug_id": delete_bugs}})
        if still_existing:
            Log.note("Ensure the following existing private bugs are deleted:\n{{private_bugs|indent}}", {"private_bugs": sorted(still_existing)})
        es.delete_record({"terms": {"bug_id": delete_bugs}})
        es_comments.delete_record({"terms": {"bug_id": delete_bugs}})


    #RECENT PUBLIC BUGS
    possible_public_bugs = get_recent_private_bugs(db, param)
    if param.allow_private_bugs:
        #PRIVATE BUGS
        #    A CHANGE IN PRIVACY INDICATOR MEANS THE WHITEBOARD IS AFFECTED, REDO
        es.delete_record({"terms": {"bug_id": possible_public_bugs}})
    else:
        #PUBLIC BUGS
        #    IF ADDING GROUP THEN private_bugs ALREADY DID THIS
        #    IF REMOVING GROUP THEN NO RECORDS TO DELETE
        pass

    #REMOVE **RECENT** PRIVATE ATTACHMENTS
    private_attachments = get_recent_private_attachments(db, param)
    bugs_to_refresh = set(Q.select(private_attachments, "bug_id"))
    es.delete_record({"terms": {"bug_id": bugs_to_refresh}})

    #REBUILD BUGS THAT GOT REMOVED
    bug_list = (possible_public_bugs | bugs_to_refresh) - private_bugs # REMOVE PRIVATE BUGS
    if bug_list:
        refresh_param = param.copy()
        refresh_param.bug_list = bug_list
        refresh_param.start_time = 0
        refresh_param.start_time_str = extract_bugzilla.milli2string(db, 0)

        try:
            etl(db, output_queue, refresh_param.copy(), please_stop=None)
            etl_comments(db, es_comments, refresh_param.copy(), please_stop=None)
        except Exception, e:
            Log.error("Problem with etl using parameters {{parameters}}", {
                "parameters": refresh_param
            }, e)


    #REFRESH COMMENTS WITH PRIVACY CHANGE
    private_comments = get_recent_private_comments(db, param)
    comment_list = set(Q.select(private_comments, "comment_id")) | {0}
    es_comments.delete_record({"terms": {"comment_id": comment_list}})
    changed_comments = get_comments_by_id(db, comment_list, param)
    es_comments.extend({"id": c.comment_id, "value": c} for c in changed_comments)

    #GET LIST OF CHANGED BUGS
    with Timer("time to get changed bug list"):
        if param.allow_private_bugs:
            bug_list = Q.select(db.query("""
                SELECT
                    b.bug_id
                FROM
                    bugs b
                WHERE
                    delta_ts >= {{start_time_str}}
            """, {
                "start_time_str": param.start_time_str
            }), u"bug_id")
        else:
            bug_list = Q.select(db.query("""
                SELECT
                    b.bug_id
                FROM
                    bugs b
                LEFT JOIN
                    bug_group_map m ON m.bug_id=b.bug_id
                WHERE
                    delta_ts >= {{start_time_str}} AND
                    m.bug_id IS NULL
            """, {
                "start_time_str": param.start_time_str
            }), u"bug_id")

    if not bug_list:
        return

    with Thread.run("alias analysis", alias_analysis.full_analysis, settings=settings, bug_list=bug_list):
        Log.note("Updating {{num}} bugs:\n{{bug_list|indent}}", {
            "num": len(bug_list),
            "bug_list": bug_list
        })
        param.bug_list = bug_list
        run_both_etl(**{
            "db": db,
            "output_queue": output_queue,
            "es_comments": es_comments,
            "param": param.copy()
        })


def full_etl(resume_from_last_run, settings, param, db, es, es_comments, output_queue):
    with Thread.run("alias_analysis", alias_analysis.full_analysis, settings=settings):
        end = nvl(settings.param.end, db.query("SELECT max(bug_id)+1 bug_id FROM bugs")[0].bug_id)
        start = nvl(settings.param.start, 0)
        if resume_from_last_run:
            start = nvl(settings.param.start, Math.floor(get_max_bug_id(es), settings.param.increment))

        #############################################################
        ## MAIN ETL LOOP
        #############################################################

        #TWO WORKERS IS MORE THAN ENOUGH FOR A SINGLE THREAD
        # with Multithread([run_both_etl, run_both_etl]) as workers:
        for min, max in Q.intervals(start, end, settings.param.increment):
            if settings.args.quick and min < end - settings.param.increment and min != 0:
                #--quick ONLY DOES FIRST AND LAST BLOCKS
                continue

            try:
                #GET LIST OF CHANGED BUGS
                with Timer("time to get {{min}}..{{max}} bug list", {"min":min, "max":max}):
                    if param.allow_private_bugs:
                        bug_list = Q.select(db.query("""
                            SELECT
                                b.bug_id
                            FROM
                                bugs b
                            WHERE
                                delta_ts >= {{start_time_str}} AND
                                ({{min}} <= b.bug_id AND b.bug_id < {{max}})
                        """, {
                            "min": min,
                            "max": max,
                            "start_time_str": param.start_time_str
                        }), u"bug_id")
                    else:
                        bug_list = Q.select(db.query("""
                            SELECT
                                b.bug_id
                            FROM
                                bugs b
                            LEFT JOIN
                                bug_group_map m ON m.bug_id=b.bug_id
                            WHERE
                                delta_ts >= {{start_time_str}} AND
                                ({{min}} <= b.bug_id AND b.bug_id < {{max}}) AND
                                m.bug_id IS NULL
                        """, {
                            "min": min,
                            "max": max,
                            "start_time_str": param.start_time_str
                        }), u"bug_id")

                if not bug_list:
                    continue

                param.bug_list = bug_list
                run_both_etl(**{
                    "db": db,
                    "output_queue": output_queue,
                    "es_comments": es_comments,
                    "param": param.copy()
                })

            except Exception, e:
                Log.error("Problem with dispatch loop in range [{{min}}, {{max}})", {
                    "min": min,
                    "max": max
                }, e)


def main(settings, es=None, es_comments=None):
    if not settings.param.allow_private_bugs and es and not es_comments:
        Log.error("Must have ES for comments")

    resume_from_last_run = File(settings.param.first_run_time).exists and not File(settings.param.last_run_time).exists

    #MAKE HANDLES TO CONTAINERS
    try:
        with DB(settings.bugzilla, readonly=True) as db:
            current_run_time, es, es_comments, last_run_time = setup_es(settings, db, es, es_comments)

            with ThreadedQueue(es, size=500, silent=True) as output_queue:
                #SETUP RUN PARAMETERS
                param = Struct()
                param.end_time = CNV.datetime2milli(get_current_time(db))
                # DB WRITES ARE DELAYED, RESULTING IN UNORDERED bug_when IN bugs_activity (AS IS ASSUMED FOR bugs(delats_ts))
                # THIS JITTER IS USUALLY NO MORE THAN ONE SECOND, BUT WE WILL GO BACK 60sec, JUST IN CASE.
                # THERE ARE OCCASIONAL WRITES THAT ARE IN GMT, BUT SINCE THEY LOOK LIKE THE FUTURE, WE CAPTURE THEM
                param.start_time = last_run_time - nvl(settings.param.look_back, 5 * 60 * 1000)  # 5 MINUTE LOOK_BACK
                param.start_time_str = extract_bugzilla.milli2string(db, param.start_time)
                param.alias_file = settings.param.alias_file
                param.allow_private_bugs = settings.param.allow_private_bugs

                if last_run_time > 0:
                    with Timer("run incremental etl"):
                        incremental_etl(settings, param, db, es, es_comments, output_queue)
                else:
                    with Timer("run full etl"):
                        full_etl(resume_from_last_run, settings, param, db, es, es_comments, output_queue)

                output_queue.add(Thread.STOP)

        if settings.es.alias:
            es.delete_all_but(settings.es.alias, settings.es.index)
            es.add_alias(settings.es.alias)

        if settings.es_comments.alias:
            es.delete_all_but(settings.es_comments.alias, settings.es_comments.index)
            es_comments.add_alias(settings.es_comments.alias)

        File(settings.param.last_run_time).write(unicode(CNV.datetime2milli(current_run_time)))
    except Exception, e:
        Log.error("Problem with main ETL loop", e)
    finally:
        try:
            close_db_connections()
        except Exception, e:
            pass
        try:
            es.set_refresh_interval(1)
        except Exception, e:
            pass

def get_bug_ids(es, filter):
    try:
        results = es.search({
            "query": {"filtered": {
                "query": {"match_all": {}},
                "filter": filter
            }},
            "from": 0,
            "size": 200000,
            "sort": [],
            "fields": ["bug_id"]
        })

        return set(results.hits.hits.fields.bug_id)
    except Exception, e:
        Log.error("Can not get_max_bug from {{host}}/{{index}}", {
            "host": es.settings.host,
            "index": es.settings.index
        }, e)



def get_max_bug_id(es):
    try:
        results = es.search({
            "query": {"filtered": {
                "query": {"match_all": {}},
                "filter": {"script": {"script": "true"}}
            }},
            "from": 0,
            "size": 0,
            "sort": [],
            "facets": {"0": {"statistical": {"field": "bug_id"}}}
        })

        if results.facets["0"].count == 0:
            return 0
        return results.facets["0"].max
    except Exception, e:
        Log.error("Can not get_max_bug from {{host}}/{{index}}", {
            "host": es.settings.host,
            "index": es.settings.index
        }, e)


def close_db_connections():
    (globals()["db_cache"], temp) = ([], db_cache)
    for db in temp:
        db.close()

    (globals()["comment_db_cache"], temp) = ([], comment_db_cache)
    for db in temp:
        db.close()


def start():
    try:
        settings = startup.read_settings(defs=[{
            "name": ["--quick", "--fast"],
            "help": "use this to process the first and last block, useful for testing the config settings before doing a full run",
            "action": "store_true",
            "dest": "quick"
        }, {
            "name": ["--restart", "--reset", "--redo"],
            "help": "use this to force a reprocessing of all data",
            "action": "store_true",
            "dest": "restart"
        }])

        with startup.SingleInstance(flavor_id=settings.args.filename):
            if settings.args.restart:
                for l in struct.listwrap(settings.debug.log):
                    if l.filename:
                        File(l.filename).parent.delete()
                File(settings.param.first_run_time).delete()
                File(settings.param.last_run_time).delete()

            Log.start(settings.debug)
            main(settings)
    except Exception, e:
        Log.fatal("Can not start", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    start()
