################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################

## REPLACES THE KETTLE FLOW CONTROL PROGRAM, AND BASH SCRIPT


from datetime import datetime, timedelta
import functools
from bzETL import parse_bug_history, transform_bugzilla
from bzETL.extract_bugzilla import get_private_bugs, get_recent_private_attachments, get_recent_private_comments, get_comments
from bzETL.util.basic import nvl
from bzETL.util.maths import Math

from extract_bugzilla import get_bugs, get_dependencies, get_flags, get_new_activities, get_bug_see_also, get_attachments, get_keywords, get_cc, get_bug_groups, get_duplicates
from parse_bug_history import parse_bug_history_
from bzETL.util import struct
from bzETL.util.logs import Log
from bzETL.util.struct import Struct, Null
from bzETL.util.files import File
from bzETL.util.startup import startup
from bzETL.util.threads import Queue, Thread, AllThread
from bzETL.util.cnv import CNV
from bzETL.util.elasticsearch import ElasticSearch
from bzETL.util.query import Q
from bzETL.util.db import DB, SQL


db_cache = []
comment_db_cache = []

#HERE ARE ALL THE FUNCTIONS WE WANT TO RUN, IN PARALLEL
get_stuff_from_bugzilla = [
    get_bugs,
    get_dependencies,
    get_flags,
    get_new_activities,
    get_bug_see_also,
    get_attachments,
    get_keywords,
    get_cc,
    get_bug_groups,
    get_duplicates
]



def etl_comments(db, es, param):
    # CONNECTIONS ARE EXPENSIVE, CACHE HERE
    if len(comment_db_cache) == 0:
        comment_db_cache.append(DB(db))

    def temp():
        comments=get_comments(comment_db_cache[0], param)
        es.add([{"id":c.comment_id, "value":c} for c in comments])
    return Thread.run(temp)



#MIMIC THE KETTLE GRAPHICAL PROGRAM
def etl(db, es, param):
    # CONNECTIONS ARE EXPENSIVE, CACHE HERE
    if len(db_cache) == 0:
        db_cache.extend([DB(db) for f in get_stuff_from_bugzilla])

    # ASYMMETRIC MULTI THREADING
    db_results=Queue()
    with AllThread() as all:
        for i,f in enumerate(get_stuff_from_bugzilla):
            def process(target, db, param):
                db_results.extend(target(db, param))

            all.add(process, f, db_cache[i], param)
    db_results.add(Thread.STOP)

    output_queue = Queue()
    #TODO: USE SEPARATE THREAD TO SORT AND PROCESS BUG CHANGE RECORDS
    process = parse_bug_history_(param, output_queue)

    sorted = Q.sort(db_results, [
        "bug_id",
        "_merge_order",
        {"field":"modified_ts", "sort":-1},
        "modified_by"
    ])
    for s in sorted:
        process.processRow(s)
    process.processRow(struct.wrap({"bug_id": parse_bug_history.STOP_BUG, "_merge_order": 1}))
    output_queue.add(Thread.STOP)

    #USE MAIN THREAD TO SEND TO ES
    #output_queue IS A MULTI-THREADED QUEUE, SO THIS WILL BLOCK UNTIL THE 10K ARE READY
    for i, g in Q.groupby(output_queue, size=10000):
        Log.note("write {{num}} block to es", {"num":len(g)})
        es.add({"id": x.id, "value": x} for x in g)

    process.saveAliases()
    return "done"


def main(settings, es=Null, es_comments=Null):
    if not settings.param.allow_private_bugs and es!=Null and es_comments==Null:
        Log.error("Must have ES for comments")

    current_run_time=datetime.utcnow()

    #MAKE HANDLES TO CONTAINERS
    try:
        with DB(settings.bugzilla) as db:
            if settings.args.resume:
                last_run_time = 0
                current_run_time = datetime.utcnow() - timedelta(days=1)
                if es == Null:
                    if settings.es.alias == Null:
                        settings.es.alias = settings.es.index
                        temp = Q.run({
                            "from":ElasticSearch(settings.es).get_aliases(),
                            "select":"index",
                            "where":lambda(r):r.alias==settings.es.alias,
                            "sort":"index"
                        })
                        settings.es.index = temp[-1]
                    es = ElasticSearch(settings.es)
                    es.set_refresh_interval(1)

                    if settings.es_comments.alias == Null:
                        settings.es_comments.alias = settings.es_comments.index
                        settings.es_comments.index = Q.run({
                            "from":ElasticSearch(settings.es_comments).get_aliases(),
                            "select":"index",
                            "where":lambda(r):r.alias==settings.es_comments.alias,
                            "sort":"index"
                        })[-1]
                    es_comments = ElasticSearch(settings.es_comments)
            elif settings.param.incremental:
                last_run_time = long(File(settings.param.last_run_time).read())
                if es == Null:
                    es = ElasticSearch(settings.es)
                    es_comments = ElasticSearch(settings.es_comments)
            else:
                last_run_time=0
                if es == Null:
                    schema=File(settings.es.schema_file).read()
                    if transform_bugzilla.USE_ATTACHMENTS_DOT:
                        schema = schema.replace("attachments_", "attachments.")

                    if settings.es.alias == Null:
                        settings.es.alias = settings.es.index
                        settings.es.index = settings.es.alias + CNV.datetime2string(datetime.utcnow(), "%Y%m%d_%H%M%S")
                    es = ElasticSearch.create_index(settings.es, schema)

                    if settings.es_comments.alias == Null:
                        settings.es_comments.alias = settings.es_comments.index
                        settings.es_comments.index = settings.es_comments.alias + CNV.datetime2string(datetime.utcnow(), "%Y%m%d_%H%M%S")
                    es_comments = ElasticSearch.create_index(settings.es_comments, File(settings.es_comments.schema_file).read())


            #SETUP RUN PARAMETERS
            param = Struct()
            param.end_time = CNV.datetime2milli(datetime.utcnow())
            param.start_time = last_run_time
            param.alias_file = settings.param.alias_file


            param.allow_private_bugs=settings.param.allow_private_bugs

            private_bugs = get_private_bugs(db, param)

            if settings.param.incremental:
                ####################################################################
                ## ES TAKES TIME TO DELETE RECORDS, DO DELETE FIRST WITH HOPE THE
                ## INDEX GETS A REWRITE DURING ADD OF NEW RECORDS
                ####################################################################

                #REMOVE PRIVATE BUGS
                es.delete_record({"terms": {"bug_id": private_bugs}})

                #REMOVE **RECENT** PRIVATE ATTACHMENTS
                private_attachments = get_recent_private_attachments(db, param)
                bugs_to_refresh = set([a.bug_id for a in private_attachments])
                es.delete_record({"terms": {"bug_id": bugs_to_refresh}})

                #REBUILD BUGS THAT GOT REMOVED
                bug_list = bugs_to_refresh - private_bugs # BUT NOT PRIVATE BUGS
                if len(bug_list) > 0:
                    refresh_param = param.copy()
                    refresh_param.bug_list = SQL(bug_list)
                    refresh_param.start_time = 0

                    try:
                        etl(db, es, refresh_param)
                    except Exception, e:
                        Log.error("Problem with etl using parameters {{parameters}}", {
                            "parameters": refresh_param
                        }, e)

                #REMOVE PRIVATE COMMENTS
                private_comments=get_recent_private_comments(db, param)
                comment_list = Q.select(private_comments, "comment_id")
                es_comments.delete_record({"terms": {"comment_id": comment_list}})


            ########################################################################
            ## MAIN ETL LOOP
            ########################################################################

            end = nvl(settings.param.end, db.query("SELECT max(bug_id)+1 bug_id FROM bugs")[0].bug_id)
            start = nvl(settings.param.start, 0)
            if settings.args.resume:
                start = nvl(settings.param.start, Math.floor(get_max_bug_id(es), settings.param.increment))
            #WE SHOULD SPLIT THIS OUT INTO PROCESSES FOR GREATER SPEED!!
            for b in range(start, end, settings.param.increment):
                (min, max)=(b, b+settings.param.increment)
                try:
                    bug_list=Q.select(db.query("""
                        SELECT
                            bug_id
                        FROM
                            bugs
                        WHERE
                            delta_ts >= CONVERT_TZ(FROM_UNIXTIME({{start_time}}/1000), 'UTC', 'US/Pacific') AND
                            ({{min}} <= bug_id AND bug_id < {{max}}) AND
                            bug_id not in {{private_bugs}}
                        """, {
                            "min":min,
                            "max":max,
                            "private_bugs":SQL(private_bugs),
                            "start_time":param.start_time
                    }), u"bug_id")

                    if len(bug_list) == 0:
                        continue

                    param.bug_list=SQL(bug_list)

                    comment_thread=etl_comments(db, es_comments, param)
                    etl(db, es, param)
                    comment_thread.join()



                except Exception, e:
                    Log.warning("Problem with etl in range [{{min}}, {{max}})", {
                        "min":min,
                        "max":max
                    }, e)

        if settings.es.alias != Null:
            es.delete_all_but(settings.es.alias, settings.es.index)

        File(settings.param.last_run_time).write(unicode(CNV.datetime2milli(current_run_time)))

    finally:
        close_db_connections()


def get_max_bug_id(es):
    try:
        results = es.search({
            "query": {"filtered": {
                "query": {"match_all": {}},
                "filter": {"script": {"script":"true"}}
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
        Log.error("Can not get_max_bug from {{host}}/{{index}}",{
            "host": es.settings.host,
            "index": es.settings.index
        }, e)






def close_db_connections():
    (globals()["db_cache"], temp)=([], db_cache)
    for db in temp:
        db.close()

    (globals()["comment_db_cache"], temp)=([], comment_db_cache)
    for db in temp:
        db.close()



def start():
    try:
        settings = startup.read_settings(defs={
            "name": "--resume",
            "help": "set to true to resume from incomplete previous run",
            "action": "store_true",
            "dest": "resume"
        })
        Log.start(settings.debug)
        main(settings)
    except Exception, e:
        Log.error("Problems exist", e)
    finally:
        Log.stop()



def profile_etl():
    import profile
    profile.run("start()")



if __name__=="__main__":
    # profile_etl()
    start()
