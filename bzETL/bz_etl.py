################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################

## REPLACES THE KETTLE FLOW CONTROL PROGRAM, AND BASH SCRIPT

from datetime import datetime

from util.files import File
from util.startup import startup
from util.strings import expand_template
from util.threads import Queue, Thread
from util.cnv import CNV
from util.elasticsearch import ElasticSearch
from util.multithread import Multithread
from parse_bug_history import parse_bug_history_
from util.query import Q
from util.db import DB, SQL
from extract_bugzilla import *

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
    sorted=Q.sort(output, ["bug_id", "_merge_order", {"field":"modified_ts", "sort":-1}])

    #USE SEPARATE THREAD TO SORT AND PROCESS BUG CHANGE RECORDS
    process=parse_bug_history_(param, output_queue)
    for s in sorted:
        process.processRow(s)
    process.processRow(Struct(bug_id=999999999, _merge_order=1))
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

        if settings.es.alias is None:
            settings.es.alias=settings.es.index
            settings.es.index=settings.es.alias+CNV.datetime2string(datetime.utcnow(), "%Y%m%d_%H%M%S")
        es=ElasticSearch.create_index(settings.es, File(settings.es.schema_file).read())


        #SETUP RUN PARAMETERS
        param=Struct()
        param.BUGS_TABLE_COLUMNS=get_bugs_table_columns(db, settings.bugzilla.schema)
        param.BUGS_TABLE_COLUMNS_SQL=SQL(",\n".join(["`"+c.column_name+"`" for c in param.BUGS_TABLE_COLUMNS]))
        param.BUGS_TABLE_COLUMNS=Q.select(param.BUGS_TABLE_COLUMNS, "column_name")
        param.END_TIME=CNV.datetime2milli(datetime.utcnow())
        param.START_TIME=0
        param.alias_file=settings.param.alias_file
        param.end=db.query("SELECT max(bug_id)+1 bug_id FROM bugs")[0].bug_id


        #WE SHOULD SPLIT THIS OUT INTO PROCESSES FOR GREATER SPEED!!
        for b in range(settings.param.start, param.end, settings.param.increment):
            try:
                (min, max)=(b, b+settings.param.increment)
                param.BUG_IDS_PARTITION=SQL("(bug_id>={{min}} and bug_id<{{max}})", {
                    "min":min,
                    "max":max
                })
                etl(db, es, param)
            except Exception, e:
                D.warning("Problem with etl in range [{{min}}, {{max}})", {
                    "min":min,
                    "max":max
                })


if __name__=="__main__":
    #import profile
    #profile.run("""
    settings=startup.read_settings()
    D.start(settings.debug)
    main(settings)
    D.stop()
    #""")


