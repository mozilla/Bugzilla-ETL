################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################

## REPLACES THE KETTLE FLOW CONTROL PROGRAM, AND BASH SCRIPT

from datetime import datetime
from bzETL.extract_bugzilla import get_bugs_table_columns, get_private_bugs, get_private_attachments, get_recent_private_attachments

from .bzReplicate import get_last_updated
from .extract_bugzilla import get_bugs, get_dependencies,get_flags,get_new_activities,get_bug_see_also,get_attachments,get_keywords,get_cc,get_bug_groups,get_duplicates
from .parse_bug_history import parse_bug_history_
from bzETL.util import struct
from bzETL.util.logs import Log
from bzETL.util.struct import Struct
from bzETL.util.files import File
from bzETL.util.startup import startup
from bzETL.util.threads import Queue, Thread
from bzETL.util.cnv import CNV
from bzETL.util.elasticsearch import ElasticSearch
from bzETL.util.multithread import Multithread
from bzETL.util.query import Q
from bzETL.util.db import DB, SQL

db_cache=[]



#MIMIC THE KETTLE GRAPHICAL PROGRAM
def etl(db, es, param):
    #HERE ARE ALL THE FUNCTIONS WE WANT TO RUN, IN PARALLEL
    funcs=[
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

    # CONNECTIONS ARE EXPENSIVE, CACHE HERE
    if len(db_cache)==0:
        db_cache.extend([DB(db) for f in funcs])


    #GIVE THEM ALL THE SAME PARAMETERS
    output=[]
    with Multithread(funcs) as multi:
        params=[{"db":db_cache[i], "param":param} for i, f in enumerate(funcs)]
        responses=multi.execute(params)

        #CONCAT ALL RESPONSES (BLOCKS UNTIL ALL RETRIEVED)
        for r in responses:
            output.extend(r)

    output_queue=Queue()
    sorted=Q.sort(output, ["bug_id", "_merge_order", {"field":"modified_ts", "sort":-1}, "modified_by"])

    #USE SEPARATE THREAD TO SORT AND PROCESS BUG CHANGE RECORDS
    process=parse_bug_history_(param, output_queue)
    for s in sorted:
        process.processRow(s)
    process.processRow(struct.wrap({"bug_id":999999999, "_merge_order":1}))
    output_queue.add(Thread.STOP)

    #USE MAIN THREAD TO SEND TO ES
    #output_queue IS A MULTI-THREADED QUEUE, SO THIS WILL BLOCK UNTIL THE 10K ARE READY
    for i, g in Q.groupby(output_queue, size=10000):
        es.add({"id":x.id, "value":x} for x in g)

    return "done"



#def test(settings):
#    funcs=[etl for i in range(8)]  #USE ALL 8 PROCESSORS
#
#    queue=Multiprocess.Queue()
#    with Multiprocess(queue, funcs) as multi
#        for b in range(settings.param.start, settings.param.end, settings.param.increment):
#            param.BUG_IDS_PARTITION=SQL(expand_template("(bug_id>={{min}} and bug_id<{{min}})", {
#                "min":b,
#                "max":b+settings.param.increment
#            }))
#            multi.execute(param)
#
#    multi.join()
#
#


def main(settings):

    #MAKE HANDLES TO CONTAINERS
    with DB(settings.bugzilla) as db:

        if settings.param.incremental:
            es=ElasticSearch(settings.es)
            start_time=CNV.datetime2milli(get_last_updated(es))
        else:
            if settings.es.alias is None:
                settings.es.alias=settings.es.index
                settings.es.index=settings.es.alias+CNV.datetime2string(datetime.utcnow(), "%Y%m%d_%H%M%S")
            es=ElasticSearch.create_index(settings.es, File(settings.es.schema_file).read())
            start_time=0

        #SETUP RUN PARAMETERS
        param=Struct()
        param.BUGS_TABLE_COLUMNS=get_bugs_table_columns(db, settings.bugzilla.schema)
        param.BUGS_TABLE_COLUMNS_SQL=SQL(",\n".join(["`"+c.column_name+"`" for c in param.BUGS_TABLE_COLUMNS]))
        param.BUGS_TABLE_COLUMNS=Q.select(param.BUGS_TABLE_COLUMNS, "column_name")
        param.END_TIME=CNV.datetime2milli(datetime.utcnow())
        param.START_TIME=start_time
        param.alias_file=settings.param.alias_file
        param.end=db.query("SELECT max(bug_id)+1 bug_id FROM bugs")[0].bug_id


        ########################################################################
        ## ES TAKES TIME TO DELETE RECORDS, DO DELETE FIRST WITH HOPE THE
        ## INDEX GETS A REWRITE DURING ADD OF NEW RECORDS
        ########################################################################

        #REMOVE PRIVATE BUGS
        private_bugs=get_private_bugs(db, param)
        es.delete_record({"terms":{"bug_id":private_bugs}})

        #REMOVE **RECENT** PRIVATE ATTACHMENTS
        private_attachments=get_recent_private_attachments(db, param)
        bugs_to_refresh=set([a.bug_id for a in private_attachments])
        es.delete_record({"terms":{"bug_id":bugs_to_refresh}})
        
        #REBUILD BUGS THAT GOT REMOVED
        refresh_param=Struct(**param)
        refresh_param.BUG_IDS_PARTITION=SQL("bug_id in {{bugs_to_refresh}}", {
            "bugs_to_refresh":bugs_to_refresh-private_bugs #BUT NOT PRIVATE BUGS
        })
        refresh_param.START_TIME=0

        try:
            etl(db, es, refresh_param)
        except Exception, e:
            Log.warning("Problem with etl using paremeters {{parameters}}", {
                "parameters":refresh_param
            }, e)


        ########################################################################
        ## MAIN ETL LOOP
        ########################################################################

        #WE SHOULD SPLIT THIS OUT INTO PROCESSES FOR GREATER SPEED!!
        for b in range(settings.param.start, param.end, settings.param.increment):
            (min, max)=(b, b+settings.param.increment)
            try:
                param.BUG_IDS_PARTITION=SQL("(bug_id>={{min}} and bug_id<{{max}}) and bug_id not in {{private_bugs}}", {
                    "min":min,
                    "max":max,
                    "private_bugs":private_bugs
                })
                etl(db, es, param)
            except Exception, e:
                Log.warning("Problem with etl in range [{{min}}, {{max}})", {
                    "min":min,
                    "max":max
                }, e)


def start():
    #import profile
    #profile.run("""
    try:
        settings=startup.read_settings()
        Log.start(settings.debug)
        main(settings)
    except Exception, e:
        Log.error("Problems exist", e)
    finally:
        Log.stop()
    #""")


if __name__=="__main__":
    start()
