from datetime import datetime
from multiprocessing import Queue
from string import Template
from threading import Thread
from util.cnv import CNV
from util.elasticsearch import ElasticSearch
from util.map import Map
from util.multithread import Multithread
from util.query import Q
from util.startup import startup
import parse_bug_history
from util.debug import D
from util.db import DB, SQL


def etl(db, es, param):
    #HERE ARE ALL TEH FUNCTIONS WE WANT TO RUN, IN PARALLEL
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
        get_duplicates,
        get_dependencies,
    ]
    #GIVE THEM ALL THE SAME PARAMETERS
    with Multithread(funcs) as multi:
        responses=multi.execute([{"db":db, "param":param} for i in range(0, len(funcs))])

    #CONCAT ALL RESPONSES
    output=[]
    for r in responses:
        output.extend(r)

    #USE SEPARATE THREAD TO SORT AND PROCESS BUG CHANGE RECORDS
    process=parse_bug_history.setup(param, Queue())
    def func():
        sorted=Q.sort(output, ["bug_id", "_merge_order"])
        for s in sorted: process(s)
    Thread.run(func)

    #process.output IS A MUTITHREADED QUEUE, SO THIS WILL BLOCK UNTIL THE 10K ARE READY
    for i, g in Q.groupby(process.output, size=10000):
        es.push(map(lambda(x):{"id":x._id, "json":x}, g))






def get_bugs(db, param):
    bugs=db.query("""
        SELECT bug_id
            , UNIX_TIMESTAMP(CONVERT_TZ(b.creation_ts, 'US/Pacific','UTC'))*1000 AS modified_ts
            , pr.login_name AS modified_by
            , UNIX_TIMESTAMP(CONVERT_TZ(b.creation_ts, 'US/Pacific','UTC'))*1000 AS created_ts
            , pr.login_name AS created_by
            , pa.login_name AS assigned_to
            , pq.login_name AS qa_contact
            , prod.`name` AS product
            , comp.`name` AS component
            , ${BUGS_TABLE_COLUMNS}
        FROM bugs b
            LEFT JOIN profiles pr ON b.reporter = pr.userid
            LEFT JOIN profiles pa ON b.assigned_to = pa.userid
            LEFT JOIN profiles pq ON b.qa_contact = pq.userid
            LEFT JOIN products prod ON prod.id = product_id
            LEFT JOIN components comp ON comp.id = component_id
        WHERE
            UNIX_TIMESTAMP(CONVERT_TZ(delta_ts, 'US/Pacific','UTC'))*1000 > ${START_TIME}
            AND ${BUG_IDS_PARTITION}
        """, param)

    #bugs IS LIST OF BUGS WHICH MUST BE CONVERTED TO THE DELTA RECORDS FOR ALL FIELDS
    bugs_fields = param.BUGS_TABLE_COLUMNS.split(",")
    output=[]
    for r in bugs:
        flatten_bugs_record(r, bugs_fields, output)

    return output



def flatten_bugs_record(r, bugs_fields, output):
    newRow=Map()
    newRow.bug_id=r.bug_id
    newRow.modified_ts=r.modified_ts
    newRow.modified_by=r.modified_by
    newRow.field_name="created_ts"
    newRow.field_value=r.created_ts
    newRow._merge_order=1L
    output.append(newRow)

    newRow=Map()
    newRow.bug_id=r.bug_id
    newRow.modified_ts=r.modified_ts
    newRow.modified_by=r.modified_by
    newRow.field_name="created_by"
    newRow.field_value=r.created_by
    newRow._merge_order=1L
    output.append(newRow)

    newRow=Map()
    newRow.bug_id=r.bug_id
    newRow.modified_ts=r.modified_ts
    newRow.modified_by=r.modified_by
    newRow.field_name="assigned_to"
    newRow.field_value=r.assigned_to
    newRow._merge_order=1L
    output.append(newRow)

    newRow=Map()
    newRow.bug_id=r.bug_id
    newRow.modified_ts=r.modified_ts
    newRow.modified_by=r.modified_by
    newRow.field_name="qa_contact"
    newRow.field_value=r.qa_contact
    newRow._merge_order=1L
    output.append(newRow)

    newRow=Map()
    newRow.bug_id=r.bug_id
    newRow.modified_ts=r.modified_ts
    newRow.modified_by=r.modified_by
    newRow.field_name="product"
    newRow.field_value=r.product
    newRow._merge_order=1L
    output.append(newRow)

    newRow=Map()
    newRow.bug_id=r.bug_id
    newRow.modified_ts=r.modified_ts
    newRow.modified_by=r.modified_by
    newRow.field_name="component"
    newRow.field_value=r.component
    newRow._merge_order=1L
    output.append(newRow)

    for field_name in bugs_fields:
        value = r[field_name]
        if value != "---":
            newRow=Map()
            newRow.bug_id=r.bug_id
            newRow.modified_ts=r.modified_ts
            newRow.modified_by=r.modified_by
            newRow.field_name=field_name
            newRow.field_value=value
            newRow._merge_order=1L
            output.append(newRow)

    return output







def get_dependencies(db, param):
    return db.query("""
        SELECT blocked AS bug_id
            , CAST(null AS signed) AS modified_ts
            , CAST(null AS char(255)) AS modified_by
            , 'dependson' AS field_name
            , CAST(dependson AS char(255)) AS field_value
            , CAST(null AS char(255)) AS field_value_removed
            , CAST(null AS signed) AS attach_id
            , 2 AS _merge_order
        FROM dependencies d
        WHERE blocked IN (
            SELECT bug_id FROM bugs WHERE
            UNIX_TIMESTAMP(CONVERT_TZ(delta_ts, 'US/Pacific','UTC'))*1000 > ${START_TIME}
            AND ${BUG_IDS_PARTITION}
            )
        UNION
        SELECT dependson
            , null
            , null
            , 'blocked'
            , CAST(blocked AS char(255))
            , null
            , null
            , 2
        FROM dependencies d
        WHERE dependson IN (
            SELECT bug_id FROM bugs WHERE
            UNIX_TIMESTAMP(CONVERT_TZ(delta_ts, 'US/Pacific','UTC'))*1000 > ${START_TIME}
            AND ${BUG_IDS_PARTITION}
            )
        ORDER BY bug_id
    """, param)


def get_duplicates(db, param):
    return db.query("""
        SELECT dupe AS bug_id
            , CAST(null AS signed) AS modified_ts
            , CAST(null AS char(255)) AS modified_by
            , 'dupe_of' AS field_name
            , CAST(dupe_of AS char(255)) AS field_value
            , CAST(null AS char(255)) AS field_value_removed
            , CAST(null AS signed) AS attach_id
            , 2 AS _merge_order
        FROM duplicates d
        WHERE dupe IN (
            SELECT bug_id FROM bugs WHERE
            UNIX_TIMESTAMP(CONVERT_TZ(delta_ts, 'US/Pacific','UTC'))*1000 > ${START_TIME}
            AND ${BUG_IDS_PARTITION}
            )
        UNION
        SELECT dupe_of
            , null
            , null
            , 'dupe_by'
            , CAST(dupe AS char(255))
            , null
            , null
            , 2
        FROM duplicates d
        WHERE dupe_of IN (
            SELECT bug_id FROM bugs WHERE
            UNIX_TIMESTAMP(CONVERT_TZ(delta_ts, 'US/Pacific','UTC'))*1000 > ${START_TIME}
            AND ${BUG_IDS_PARTITION}
            )
        ORDER BY bug_id
    """, param)


def get_bug_groups(db, param):
    return db.query("""
        SELECT bug_id
            , CAST(null AS signed) AS modified_ts
            , CAST(null AS char(255)) AS modified_by
            , 'bug_group' AS field_name
            , CAST(g.`name` AS char(255)) AS field_value
            , CAST(null AS char(255)) AS field_value_removed
            , CAST(null AS signed) AS attach_id
            , 2 AS _merge_order
        FROM bug_group_map bg
        JOIN groups g ON bg.group_id = g.id
        WHERE bug_id IN (
            SELECT bug_id FROM bugs WHERE
            UNIX_TIMESTAMP(CONVERT_TZ(delta_ts, 'US/Pacific','UTC'))*1000 > ${START_TIME}
            AND ${BUG_IDS_PARTITION}
            )
        ORDER BY bug_id
    """, param)



def get_cc(db, param):
    return db.query("""
        SELECT bug_id
            , CAST(null AS signed) AS modified_ts
            , CAST(null AS char(255)) AS modified_by
            , 'cc' AS field_name
            , CAST(p.login_name AS char(255)) AS field_value
            , CAST(null AS char(255)) AS field_value_removed
            , CAST(null AS signed) AS attach_id
            , 2 AS _merge_order
        FROM cc
        JOIN profiles p ON cc.who = p.userid
        WHERE bug_id IN (
            SELECT bug_id FROM bugs WHERE
            UNIX_TIMESTAMP(CONVERT_TZ(delta_ts, 'US/Pacific','UTC'))*1000 > ${START_TIME}
            AND ${BUG_IDS_PARTITION}
            )
        ORDER BY bug_id
    """, param)



def get_keywords(db, param):
    return db.query("""
        SELECT bug_id
            , CAST(null AS signed) AS modified_ts
            , CAST(null AS char(255)) AS modified_by
            , 'keywords' AS field_name
            , CAST(kd.`name` AS char(255)) AS field_value
            , CAST(null AS char(255)) AS field_value_removed
            , CAST(null AS signed) AS attach_id
            , 2 AS _merge_order
        FROM keywords k
        JOIN keyworddefs kd ON k.keywordid = kd.id
        WHERE bug_id IN (
            SELECT bug_id FROM bugs WHERE
            UNIX_TIMESTAMP(CONVERT_TZ(delta_ts, 'US/Pacific','UTC'))*1000 > ${START_TIME}
            AND ${BUG_IDS_PARTITION}
            )
        ORDER BY bug_id
    """, param)


def get_attachments(db, param):
    return db.query("""
        SELECT bug_id
            , UNIX_TIMESTAMP(CONVERT_TZ(a.creation_ts, 'US/Pacific','UTC'))*1000 AS modified_ts
            , login_name AS modified_by
            , UNIX_TIMESTAMP(CONVERT_TZ(a.creation_ts, 'US/Pacific','UTC'))*1000 AS created_ts
            , login_name AS created_by
            , ispatch AS 'attachments.ispatch'
            , isobsolete AS 'attachments.isobsolete'
            , isprivate AS 'attachments.isprivate'
            , attach_id
        FROM
            attachments a
            JOIN profiles p ON a.submitter_id = p.userid
        WHERE bug_id IN (
            SELECT bug_id FROM bugs WHERE
            UNIX_TIMESTAMP(CONVERT_TZ(delta_ts, 'US/Pacific','UTC'))*1000 > ${START_TIME}
            AND ${BUG_IDS_PARTITION}
            )
        ORDER BY
            bug_id,
            attach_id,
            a.creation_ts
    """, param)


def get_bug_see_also(db, param):
    return db.query("""
        SELECT bug_id
            , CAST(null AS signed) AS modified_ts
            , CAST(null AS char(255)) AS modified_by
            , 'see_also' AS field_name
            , CAST(`value` AS char(255)) AS field_value
            , CAST(null AS char(255)) AS field_value_removed
            , CAST(null AS signed) AS attach_id
            , 2 AS _merge_order
        FROM bug_see_also
        WHERE bug_id IN (
            SELECT bug_id FROM bugs WHERE
            UNIX_TIMESTAMP(CONVERT_TZ(delta_ts, 'US/Pacific','UTC'))*1000 > ${START_TIME}
            AND ${BUG_IDS_PARTITION}
            )
        ORDER BY bug_id
    """, param)

def get_new_activities(db, param):
    return db.query("""
        SELECT a.bug_id
            , UNIX_TIMESTAMP(CONVERT_TZ(bug_when, 'US/Pacific','UTC'))*1000 AS modified_ts
            , login_name AS modified_by
            , field.`name` AS field_name
            , added AS field_value
            , removed AS field_value_removed
            , attach_id
            , 9 AS _merge_order
        FROM
            sanitized_bugs_activity a
            INNER JOIN (SELECT bug_id FROM bugs WHERE
            UNIX_TIMESTAMP(CONVERT_TZ(delta_ts, 'US/Pacific','UTC'))*1000 > ${START_TIME}
            AND ${BUG_IDS_PARTITION}
            ) b ON a.bug_id = b.bug_id
            JOIN fielddefs field ON a.fieldid = field.`id`
            JOIN profiles p ON a.who = p.userid
        ORDER BY
            bug_id,
            bug_when DESC,
            attach_id
    """, param)


def get_flags(db, param):
    return db.query("""
        SELECT bug_id
            , UNIX_TIMESTAMP(CONVERT_TZ(f.creation_date, 'US/Pacific','UTC'))*1000 AS modified_ts
            , ps.login_name AS modified_by
            , 'flagtypes.name' AS field_name
            , CONCAT(ft.`name`,status,IF(requestee_id IS NULL,'',CONCAT('(',pr.login_name,')'))) AS field_value
            , CAST(null AS char(255)) AS field_value_removed
            , attach_id
            , 8 AS _merge_order
        FROM
            flags f
        JOIN `flagtypes` ft ON f.type_id = ft.id
        JOIN profiles ps ON f.setter_id = ps.userid
        LEFT JOIN profiles pr ON f.requestee_id = pr.userid
        WHERE bug_id IN (
            SELECT bug_id FROM bugs WHERE
            UNIX_TIMESTAMP(CONVERT_TZ(delta_ts, 'US/Pacific','UTC'))*1000 > ${START_TIME}
            AND ${BUG_IDS_PARTITION}
            )
        ORDER BY
            bug_id
    """, param)


settings=startup.read_settings()
D.settings(settings.debug)
db=DB(settings.bugzilla)
es=ElasticSearch(settings.es)



param=Map()
param.BUGS_TABLE_COLUMNS=Q.select(db.query("""
    SELECT column_name
    FROM information_schema.columns
    WHERE
        table_schema=${schema} AND
        table_name='bugs' AND
        column_name NOT IN ('bug_id','delta_ts','lastdiffed','creation_ts','reporter','assigned_to','qa_contact','product_id','component_id')
""", {"schema":settings.bugzilla.schema}), "column_name")
param.END_TIME=CNV.datetime2unixmilli(datetime.utcnow())
param.START_TIME=0

max_id=db.query("SELECT max(bug_id) bug_id FROM bugs")[0].bug_id
for b in range(0, max_id, settings.param.INCREMENT):
    param.BUG_IDS_PARTITION=SQL(Template("(bug_id>=${min} and bug_id<${max})").substitute({
        "min":b,
        "max":b+settings.param.INCREMENT
    }))
    etl(db, es, settings.param)