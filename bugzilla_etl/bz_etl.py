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

from mo_times.dates import unix2datetime

import jx_elasticsearch
from bugzilla_etl import extract_bugzilla, alias_analysis, parse_bug_history
from bugzilla_etl.alias_analysis import AliasAnalyzer
from bugzilla_etl.extract_bugzilla import get_comments, get_current_time, MIN_TIMESTAMP, get_private_bugs_for_delete, get_recent_private_bugs, get_recent_private_attachments, get_recent_private_comments, get_comments_by_id, get_bugs, \
    get_dependencies, get_flags, get_new_activities, get_bug_see_also, get_attachments, get_tracking_flags, get_keywords, get_tags, get_cc, get_bug_groups, get_duplicates
from bugzilla_etl.parse_bug_history import BugHistoryParser
from jx_python import jx
from mo_dots import wrap, coalesce, listwrap, Data
from mo_files import File
from mo_future import text_type, long
from mo_kwargs import override
from mo_logs import Log, startup, constants
from mo_math import Math
from mo_threads import Lock, Queue, Thread, THREAD_STOP
from mo_threads.threads import AllThread, MAIN_THREAD
from mo_times.timer import Timer
from pyLibrary import convert
from pyLibrary.env.elasticsearch import Cluster
from pyLibrary.sql.mysql import MySQL

NUM_CONNECTIONS = 4

db_cache_lock = Lock()
db_cache = []
comment_db_cache_lock = Lock()
comment_db_cache = None


#HERE ARE ALL THE FUNCTIONS WE WANT TO RUN, IN PARALLEL (b
get_stuff_from_bugzilla = [
    get_bugs,
    get_dependencies,
    get_flags,
    get_new_activities,
    get_bug_see_also,
    get_attachments,
    get_tracking_flags,
    get_keywords,
    get_tags,
    get_cc,
    get_bug_groups,
    get_duplicates
]


def etl_comments(db, output_queue, param, please_stop):
    # CONNECTIONS ARE EXPENSIVE, CACHE HERE
    global comment_db_cache
    with comment_db_cache_lock:
        if not comment_db_cache:
            comment_db = MySQL(db.settings)
            comment_db_cache = comment_db

    with comment_db_cache_lock:
        comments = get_comments(comment_db_cache, param)

    for g, block_of_comments in jx.groupby(comments, size=500):
        output_queue.extend({"id": text_type(comment.comment_id), "value": comment} for comment in block_of_comments)


def etl(db, bug_output_queue, param, alias_analyzer, please_stop):
    """
    PROCESS RANGE, AS SPECIFIED IN param AND PUSH
    BUG VERSION RECORDS TO output_queue
    """

    # MAKING CONNECTIONS ARE EXPENSIVE, CACHE HERE
    with db_cache_lock:
        if not db_cache:
            with Timer("open {{num}} connections to db", {"num": NUM_CONNECTIONS}):
                for i in range(NUM_CONNECTIONS):
                    db_cache.append(MySQL(db.settings))

    db_results = Queue(name="db results", max=2**30)

    def get_records_from_bugzilla(db, param, please_stop):
        with db.transaction():
            for get_stuff in get_stuff_from_bugzilla:
                if please_stop:
                    break
                db_results.extend(get_stuff(db, param))

    with AllThread() as all:
        with db_cache_lock:
            # SPLIT TASK EVENLY, HAVE EACH BUG USE SAME CONNECTION FOR ALL DATA
            size = Math.ceiling(len(param.bug_list)/len(db_cache))
            for g, bug_ids in jx.groupby(param.bug_list, size=size):
                param = param.copy()
                param.bug_list = bug_ids
                all.add(get_records_from_bugzilla, db_cache[g], param)
    db_results.add(THREAD_STOP)

    sorted = jx.sort(db_results, [
        "bug_id",
        "_merge_order",
        {"modified_ts": "desc"},
        "modified_by",
        {"id": "desc"}
    ])

    process = BugHistoryParser(param, alias_analyzer, bug_output_queue)
    for i, s in enumerate(sorted):
        process.processRow(s)
    process.processRow(wrap({"bug_id": parse_bug_history.STOP_BUG, "_merge_order": 1}))
    process.alias_analyzer.save_aliases()


def run_both_etl(db, bug_output_queue, comment_output_queue, param, alias_analyzer):
    comment_thread = Thread.run("etl comments", etl_comments, db, comment_output_queue, param)
    process_thread = Thread.run("etl", etl, db, bug_output_queue, param, alias_analyzer)

    result = comment_thread.join()
    if result.exception:
        Log.error("etl_comments had problems", cause=result.exception)

    result = process_thread.join()
    if result.exception:
        Log.error("etl had problems", cause=result.exception)


def setup_es(settings, db):
    """
    SETUP ES CONNECTIONS TO REFLECT IF WE ARE RESUMING, INCREMENTAL, OR STARTING OVER
    """
    current_run_time = get_current_time(db)

    if File(settings.param.first_run_time).exists and File(settings.param.last_run_time).exists:
        # INCREMENTAL UPDATE; DO NOT MAKE NEW INDEX
        last_run_time = long(File(settings.param.last_run_time).read())
        esq = jx_elasticsearch.new_instance(read_only=False, kwargs=settings.es)
        esq_comments = jx_elasticsearch.new_instance(read_only=False, kwargs=settings.es_comments)
    elif File(settings.param.first_run_time).exists:
        # DO NOT MAKE NEW INDEX, CONTINUE INITIAL FILL
        try:
            last_run_time = MIN_TIMESTAMP
            current_run_time = unix2datetime(long(File(settings.param.first_run_time).read())/1000)

            bugs = Cluster(settings.es).get_best_matching_index(settings.es.index)
            esq = jx_elasticsearch.new_instance(index=bugs.index, read_only=False, kwargs=settings.es)
            comments = Cluster(settings.es_comments).get_best_matching_index(settings.es_comments.index)
            esq_comments = jx_elasticsearch.new_instance(index=comments.index, read_only=False, kwargs=settings.es_comments)
            esq.es.set_refresh_interval(1)  #REQUIRED SO WE CAN SEE WHAT BUGS HAVE BEEN LOADED ALREADY
        except Exception as e:
            Log.warning("can not resume ETL, restarting", cause=e)
            File(settings.param.first_run_time).delete()
            return setup_es(settings, db)
    else:
        # START ETL FROM BEGINNING, MAKE NEW INDEX
        last_run_time = MIN_TIMESTAMP
        File(settings.param.first_run_time).write(text_type(convert.datetime2milli(current_run_time)))

        cluster = Cluster(settings.es)
        es = cluster.create_index(kwargs=settings.es, limit_replicas=True)
        es_comments = cluster.create_index(kwargs=settings.es_comments, limit_replicas=True)

        esq = jx_elasticsearch.new_instance(read_only=False, index=es.settings.index, kwargs=settings.es)
        esq_comments = jx_elasticsearch.new_instance(read_only=False, index=es_comments.settings.index, kwargs=settings.es_comments)

    return current_run_time, esq, esq_comments, last_run_time

@override
def incremental_etl(param, db, esq, esq_comments, bug_output_queue, comment_output_queue, kwargs):
    ####################################################################
    ## ES TAKES TIME TO DELETE RECORDS, DO DELETE FIRST WITH HOPE THE
    ## INDEX GETS A REWRITE DURING ADD OF NEW RECORDS
    ####################################################################

    # REMOVE PRIVATE BUGS
    private_bugs = get_private_bugs_for_delete(db, param)
    Log.note("Ensure the following private bugs are deleted:\n{{private_bugs|indent}}", private_bugs=sorted(private_bugs))
    for g, delete_bugs in jx.groupby(private_bugs, size=1000):
        still_existing = get_bug_ids(esq, {"terms": {"bug_id": delete_bugs}})
        if still_existing:
            Log.note("Ensure the following existing private bugs are deleted:\n{{private_bugs|indent}}", private_bugs=sorted(still_existing))
        esq.es.delete_record({"terms": {"bug_id.~n~": delete_bugs}})
        esq_comments.es.delete_record({"terms": {"bug_id.~n~": delete_bugs}})

    # RECENT PUBLIC BUGS
    possible_public_bugs = get_recent_private_bugs(db, param)
    if param.allow_private_bugs:
        #PRIVATE BUGS
        #    A CHANGE IN PRIVACY INDICATOR MEANS THE WHITEBOARD IS AFFECTED, REDO
        esq.es.delete_record({"terms": {"bug_id.~n~": possible_public_bugs}})
    else:
        #PUBLIC BUGS
        #    IF ADDING GROUP THEN private_bugs ALREADY DID THIS
        #    IF REMOVING GROUP THEN NO RECORDS TO DELETE
        pass

    # REMOVE **RECENT** PRIVATE ATTACHMENTS
    private_attachments = get_recent_private_attachments(db, param)
    bugs_to_refresh = set(jx.select(private_attachments, "bug_id"))
    esq.es.delete_record({"terms": {"bug_id.~n~": bugs_to_refresh}})

    # REBUILD BUGS THAT GOT REMOVED
    bug_list = jx.sort((possible_public_bugs | bugs_to_refresh) - private_bugs) # REMOVE PRIVATE BUGS
    if bug_list:
        refresh_param = param.copy()
        refresh_param.bug_list = bug_list
        refresh_param.start_time = MIN_TIMESTAMP
        refresh_param.start_time_str = extract_bugzilla.milli2string(db, MIN_TIMESTAMP)

        try:
            analyzer = AliasAnalyzer(kwargs.alias)
            etl(db, bug_output_queue, refresh_param.copy(), analyzer, please_stop=None)
            etl_comments(db, esq_comments.es, refresh_param.copy(), please_stop=None)
        except Exception as e:
            Log.error(
                "Problem with etl using parameters {{parameters}}",
                parameters=refresh_param,
                cause=e
            )


    # REFRESH COMMENTS WITH PRIVACY CHANGE
    private_comments = get_recent_private_comments(db, param)
    comment_list = set(jx.select(private_comments, "comment_id")) | {0}
    esq_comments.es.delete_record({"terms": {"comment_id.~n~": comment_list}})
    changed_comments = get_comments_by_id(db, comment_list, param)
    esq_comments.es.extend({"id": c.comment_id, "value": c} for c in changed_comments)

    # GET LIST OF CHANGED BUGS
    with Timer("time to get changed bug list"):
        if param.allow_private_bugs:
            bug_list = jx.select(db.query("""
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
            bug_list = jx.select(db.query("""
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

    with Thread.run("alias analysis", alias_analysis.full_analysis, kwargs=kwargs, bug_list=bug_list):
        Log.note(
            "Updating {{num}} bugs:\n{{bug_list|indent}}",
            num=len(bug_list),
            bug_list=bug_list
        )
        param.bug_list = bug_list
        run_both_etl(
            db=db,
            bug_output_queue=bug_output_queue,
            comment_output_queue=comment_output_queue,
            param=param.copy(),
            alias_analyzer=AliasAnalyzer(kwargs.alias)
        )

@override
def full_etl(resume_from_last_run, param, db, esq, esq_comments, bug_output_queue, comment_output_queue, kwargs):
    end = coalesce(param.end, db.query("SELECT max(bug_id) bug_id FROM bugs")[0].bug_id)
    start = coalesce(param.start, 0)
    if resume_from_last_run:
        # FIND THE LAST GOOD BUG NUMBER PROCESSED (WE GO BACKWARDS, SO LOOK FOR MINIMUM BUG, AND ROUND UP)
        end = coalesce(param.end, Math.ceiling(get_min_bug_id(esq), param.increment), end)
    Log.note("full etl from {{min}} to {{max}}", min=start, max=end)
    #############################################################
    ## MAIN ETL LOOP
    #############################################################
    for min, max in jx.reverse(jx.intervals(start, end, param.increment)):
        with Timer("etl block {{min}}..{{max}}", param={"min":min, "max":max}, debug=param.debug):
            if kwargs.args.quick and min < end - param.increment and min != 0:
                #--quick ONLY DOES FIRST AND LAST BLOCKS
                continue

            try:
                #GET LIST OF CHANGED BUGS
                with Timer("time to get {{min}}..{{max}} bug list", {"min":min, "max":max}):
                    if param.allow_private_bugs:
                        bug_list = jx.select(db.query("""
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
                        bug_list = jx.select(db.query("""
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
                run_both_etl(
                    db,
                    bug_output_queue,
                    comment_output_queue,
                    param.copy(),
                    alias_analyzer=AliasAnalyzer(kwargs=kwargs.alias)
                )

            except Exception as e:
                Log.error(
                    "Problem with dispatch loop in range [{{min}}, {{max}})",
                    min=min,
                    max=max,
                    cause=e
                )

@override
def main(param, es, es_comments, bugzilla, kwargs):
    param.allow_private_bugs = param.allow_private_bugs in [True, "true"]
    if not param.allow_private_bugs and es and not es_comments:
        Log.error("Must have ES for comments")

    resume_from_last_run = File(param.first_run_time).exists and not File(param.last_run_time).exists

    # MAKE HANDLES TO CONTAINERS
    try:
        with MySQL(kwargs=bugzilla, readonly=True) as db:
            current_run_time, esq, esq_comments, last_run_time = setup_es(kwargs, db)

            with esq.es.threaded_queue(max_size=500, silent=True) as output_queue:

                # def _add(value):
                #     if not isinstance(value, text_type):
                #         value = wrap(value)
                #         if value.value.bug_id==1877:
                #             Log.note("{{group}}", group= value.value.bug_group)
                #     _output_queue.add(value)
                # output_queue = Data(add=_add)

                #SETUP RUN PARAMETERS
                param_new = Data()
                param_new.end_time = convert.datetime2milli(get_current_time(db))
                # MySQL WRITES ARE DELAYED, RESULTING IN UNORDERED bug_when IN bugs_activity (AS IS ASSUMED FOR bugs(delats_ts))
                # THIS JITTER IS USUALLY NO MORE THAN ONE SECOND, BUT WE WILL GO BACK 60sec, JUST IN CASE.
                # THERE ARE OCCASIONAL WRITES THAT ARE IN GMT, BUT SINCE THEY LOOK LIKE THE FUTURE, WE CAPTURE THEM
                param_new.start_time = last_run_time - coalesce(param.look_back, 5 * 60 * 1000)  # 5 MINUTE LOOK_BACK
                param_new.start_time_str = extract_bugzilla.milli2string(db, param_new.start_time)
                param_new.alias = param.alias
                param_new.allow_private_bugs = param.allow_private_bugs
                param_new.increment = param.increment

                if last_run_time > MIN_TIMESTAMP:
                    with Timer("run incremental etl"):
                        incremental_etl(
                            param=param_new,
                            db=db,
                            esq=esq,
                            esq_comments=esq_comments,
                            bug_output_queue=output_queue,
                            comment_output_queue=esq_comments.es,
                            kwargs=kwargs
                        )
                else:
                    with Timer("run full etl"):
                        full_etl(
                            resume_from_last_run=resume_from_last_run,
                            param=param_new,
                            db=db,
                            esq=esq,
                            esq_comments=esq_comments,
                            bug_output_queue=output_queue,
                            comment_output_queue=esq_comments.es,
                            kwargs=kwargs
                        )

                output_queue.add(THREAD_STOP)

        s = Data(alias=es.index, index=esq.es.settings.index)
        if s.alias:
            esq.es.cluster.delete_all_but(s.alias, s.index)
            esq.es.add_alias(s.alias)

        s = Data(alias=es_comments.index, index=esq_comments.es.settings.index)
        if s.alias:
            esq.es.cluster.delete_all_but(s.alias, s.index)
            esq_comments.es.add_alias(s.alias)

        File(param.last_run_time).write(text_type(convert.datetime2milli(current_run_time)))
    except Exception as e:
        Log.error("Problem with main ETL loop", cause=e)
    finally:
        try:
            close_db_connections()
        except Exception as e:
            pass
        try:
            es.set_refresh_interval(1)
        except Exception as e:
            pass

def get_bug_ids(esq, filter):
    try:
        result = esq.query({"from": esq.name, "select": "bug_id", "where": filter, "limit": 20000, "format": "list"})
        return set(result.data) - {None}
    except Exception as e:
        Log.error(
            "Can not get_max_bug from {{host}}/{{index}}",
            host=esq.settings.host,
            index=esq.settings.index,
            cause=e
        )


def get_min_bug_id(esq):
    try:
        result = esq.query({"from": esq.name, "select": {"value": "bug_id", "aggregate": "min"}, "format": "list"})
        return result.data
    except Exception as e:
        Log.error(
            "Can not get_max_bug from {{host}}/{{index}}",
            host=esq.settings.host,
            index=esq.settings.index,
            cause=e
        )


def close_db_connections():
    global db_cache, comment_db_cache

    db_cache, temp = [], db_cache
    for db in temp:
        db.close()

    comment_db_cache, temp = [], comment_db_cache
    if temp:
        temp.close()


def setup():
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
        constants.set(settings.constants)

        with startup.SingleInstance(flavor_id=settings.args.filename):
            if settings.args.restart:
                for l in listwrap(settings.debug.log):
                    if l.filename:
                        File(l.filename).delete()
                File(settings.param.first_run_time).delete()
                File(settings.param.last_run_time).delete()

            Log.start(settings.debug)
            main(settings)
    except Exception as e:
        Log.fatal("Can not start", e)
    finally:
        MAIN_THREAD.stop()


if __name__ == "__main__":
    setup()
