from datetime import datetime
from util.strings import expand_template
from util.threads import Queue

from util.threads import Thread
from util.cnv import CNV
from util.elasticsearch import ElasticSearch
from util.struct import Struct
from util.multithread import Multithread
from parse_bug_history import parse_bug_history_
from util.query import Q
from util.startup import startup
from util.debug import D
from util.db import DB, SQL
from extract_bugzilla import *


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
    #GIVE THEM ALL THE SAME PARAMETERS
    with Multithread(funcs) as multi:
        params=[{"db":DB(db), "param":param} for i in range(0, len(funcs))]
        responses=multi.execute(params)

    #CONCAT ALL RESPONSES
    output=[]
    for r in responses:
        output.extend(r)

    #USE SEPARATE THREAD TO SORT AND PROCESS BUG CHANGE RECORDS
    output_queue=Queue()
    process=parse_bug_history_(param, output_queue)
    def func():
        sorted=Q.sort(output, ["bug_id", "_merge_order", {"field":"modified_ts", "sort":-1}])
        for s in sorted:
            if s.field_name=="keywords": D.println("${bug_id}: ${row}", {"bug_id":s.bug_id, "row":s})
        for s in sorted: process.processRow(s)
        process.processRow(Struct(bug_id=999999999, _merge_order=1))
        output_queue.close()
    Thread.run(func)

    #output_queue IS A MUTITHREADED QUEUE, SO THIS WILL BLOCK UNTIL THE 10K ARE READY
    for i, g in Q.groupby(output_queue, size=10000):
        es.add({"id":x._id, "value":x} for x in g)







settings=startup.read_settings()
D.settings(settings.debug)
settings.bugzilla.debug=True

db=DB(settings.bugzilla)
es=ElasticSearch(settings.es)



param=Struct()
param.BUGS_TABLE_COLUMNS=db.query("""
    SELECT
        column_name,
        column_type
    FROM
        information_schema.columns
    WHERE
        table_schema=${schema} AND
        table_name='bugs' AND
        column_name NOT IN ('bug_id','delta_ts','lastdiffed','creation_ts','reporter','assigned_to','qa_contact','product_id','component_id')
""", {"schema":settings.bugzilla.schema})
param.BUGS_TABLE_COLUMNS_SQL=SQL(",\n".join(["`"+c.column_name+"`" for c in param.BUGS_TABLE_COLUMNS]))
param.BUGS_TABLE_COLUMNS=Q.select(param.BUGS_TABLE_COLUMNS, "column_name")
param.END_TIME=CNV.datetime2unixmilli(datetime.utcnow())
param.START_TIME=0
param.alias_file=settings.param.alias_file

max_id=db.query("SELECT max(bug_id) bug_id FROM bugs")[0].bug_id
for b in range(settings.param.start, max_id, settings.param.increment):
    param.BUG_IDS_PARTITION=SQL(expand_template("(bug_id>=${min} and bug_id<${max})", {
        "min":b,
        "max":b+settings.param.increment
    }))
    etl(db, es, param)