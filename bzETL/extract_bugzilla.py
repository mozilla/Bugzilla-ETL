# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# PYTHON VERSION OF https://github.com/mozilla-metrics/bugzilla_etl/blob/master/transformations/bugzilla_to_json.ktr
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from bzETL.parse_bug_history import MAX_TIME
from bzETL.util.cnv import CNV
from bzETL.util.db import SQL

from bzETL.util.logs import Log
from bzETL.util.queries import Q
from bzETL.util.struct import Struct, Null


SCREENED_FIELDDEFS=[
    19, #bug_file_loc
    24, #short_desc
    42, #longdesc
    45, #attachments.description
    56, #alias
    64, #attachments.filename
    74, #content
    83, #attach_data.thedata
]

MIXED_CASE=[
    19, #bug_file_loc
    24  #short_desc
]

PRIVATE_ATTACHMENT_FIELD_ID=65
PRIVATE_COMMENTS_FIELD_ID=82
PRIVATE_BUG_GROUP_FIELD_ID=66

bugs_columns = Null


def get_current_time(db):
    """
    RETURN GMT TIME
    """
    output = db.query("""
        SELECT
            UNIX_TIMESTAMP(now()) `value`
        """)[0].value
    return CNV.unix2datetime(output)


def milli2string(db, value):
    """
    CONVERT GMT MILLI TO BUGZILLA DATETIME STRING
    """
    value = max(value, 0)

    output = db.query("""
        SELECT
            CAST(CONVERT_TZ(FROM_UNIXTIME({{start_time}}/1000), 'UTC', 'US/Pacific') AS CHAR) `value`
        """, {
            "start_time":value
    })[0].value
    return output

def get_bugs_table_columns(db, schema_name):

    return db.query("""
        SELECT
            column_name,
            column_type
        FROM
            information_schema.columns
        WHERE
            table_schema={{schema}} AND
            table_name='bugs' AND
            column_name NOT IN (
                'bug_id',       #EXPLICIT
                'delta_ts',     #NOT NEEDED
                'lastdiffed',   #NOT NEEDED
                'creation_ts',  #EXPLICIT
                'reporter',     #EXPLICIT
                'assigned_to',  #EXPLICIT
                'qa_contact',   #EXPLICIT
                'product_id',   #EXPLICIT
                'component_id', #EXPLICIT
                'cclist_accessible',    #NOT NEEDED
                'reporter_accessible',  #NOT NEEDED
                'short_desc',           #NOT ALLOWED
                'bug_file_loc',         #NOT ALLOWED
                'deadline',             #NOT NEEDED
                'estimated_time'       #NOT NEEDED

            )
    """, {"schema":schema_name})




def get_private_bugs(db, param):
    if param.allow_private_bugs:
        return {0}

    try:
        private_bugs=db.query("SELECT DISTINCT bug_id FROM bug_group_map")
        return set(Q.select(private_bugs, "bug_id")) | {0}
    except Exception, e:
        Log.error("problem getting private bugs", e)


def get_recent_private_bugs(db, param):
    """
    GET ONLY BUGS THAT HAVE SWITCHED PRIVACY INDICATOR
    THIS LIST IS USED TO SIGNAL BUGS THAT NEED TOTAL RE-ETL
    """
    if param.allow_private_bugs:
        return []

    param.field_id=PRIVATE_BUG_GROUP_FIELD_ID

    try:
        return db.query("""
        SELECT
            a.bug_id
        FROM
            bugs_activity a
        WHERE
            bug_when >= {{start_time_str}} AND
            fieldid={{field_id}}
        """, param)
    except Exception, e:
        Log.error("problem getting recent private attachments", e)



def get_recent_private_attachments(db, param):
    """
    GET ONLY RECENT ATTACHMENTS THAT HAVE SWITCHED PRIVACY INDICATOR
    THIS LIST IS USED TO SIGNAL BUGS THAT NEED TOTAL RE-ETL
    """
    if param.allow_private_bugs:
        return []

    param.field_id=PRIVATE_ATTACHMENT_FIELD_ID

    try:
        return db.query("""
        SELECT
            a.attach_id,
            a.bug_id
        FROM
            bugs_activity a
        WHERE
            bug_when >= {{start_time_str}} AND
            fieldid={{field_id}}
        """, param)
    except Exception, e:
        Log.error("problem getting recent private attachments", e)


def get_recent_private_comments(db, param):
    """
    GET COMMENTS THAT HAVE HAD THEIR PRIVACY INDICATOR CHANGED
    """
    if param.allow_private_bugs:
        return []

    param.field_id=PRIVATE_COMMENTS_FIELD_ID

    try:
        comments=db.query("""
            SELECT
                a.comment_id,
                a.bug_id
            FROM
                bugs_activity a
            WHERE
                bug_when >= {{start_time_str}} AND
                fieldid={{field_id}}
            """, param)

        return comments
    except Exception, e:
        Log.error("problem getting recent private attachments", e)




def get_bugs(db, param):
    try:
        if not bugs_columns:
            columns=get_bugs_table_columns(db, db.settings.schema)
            globals()["bugs_columns"] = columns

        #TODO: CF_LAST_RESOLVED IS IN PDT, FIX IT
        def lower(col):
            if col.column_type.startswith("varchar"):
                return "lower("+db.quote_column(col.column_name)+") "+db.quote_column(col.column_name)
            else:
                return db.quote_column(col.column_name)

        param.bugs_columns=Q.select(bugs_columns, "column_name")
        param.bugs_columns_SQL = SQL(",\n".join([lower(c) for c in bugs_columns]))
        param.bug_filter = db.esfilter2sqlwhere({"terms": {"bug_id": param.bug_list}})


        if param.allow_private_bugs:
            param.sensitive_columns=SQL("""
                '<screened>' short_desc,
                '<screened>' bug_file_loc
            """)
        else:
            param.sensitive_columns=SQL("""
                short_desc,
                bug_file_loc
            """)

        bugs=db.query("""
            SELECT
                bug_id,
                UNIX_TIMESTAMP(CONVERT_TZ(b.creation_ts, 'US/Pacific','UTC'))*1000 AS modified_ts,
                lower(pr.login_name) AS modified_by,
                UNIX_TIMESTAMP(CONVERT_TZ(b.creation_ts, 'US/Pacific','UTC'))*1000 AS created_ts,
                lower(pr.login_name) AS created_by,
                lower(pa.login_name) AS assigned_to,
                lower(pq.login_name) AS qa_contact,
                lower(prod.`name`) AS product,
                lower(comp.`name`) AS component,
                {{sensitive_columns}},
                {{bugs_columns_SQL}}
            FROM bugs b
                LEFT JOIN profiles pr ON b.reporter = pr.userid
                LEFT JOIN profiles pa ON b.assigned_to = pa.userid
                LEFT JOIN profiles pq ON b.qa_contact = pq.userid
                LEFT JOIN products prod ON prod.id = product_id
                LEFT JOIN components comp ON comp.id = component_id
            WHERE
                {{bug_filter}}
            """, param)

        #bugs IS LIST OF BUGS WHICH MUST BE CONVERTED TO THE DELTA RECORDS FOR ALL FIELDS
        output=[]
        for r in bugs:
            flatten_bugs_record(r, output)

        return output
    except Exception, e:
        Log.error("can not get basic bug data", e)


def flatten_bugs_record(r, output):
    for field_name, value in r.items():
        if value != "---":
            newRow=Struct()
            newRow.bug_id=r.bug_id
            newRow.modified_ts=r.modified_ts
            newRow.modified_by=r.modified_by
            newRow.field_name=field_name
            newRow.new_value=value
            newRow._merge_order=1
            output.append(newRow)






def get_dependencies(db, param):
    param.blocks_filter=db.esfilter2sqlwhere({"terms":{"blocked":param.bug_list}})
    param.dependson_filter=db.esfilter2sqlwhere({"terms":{"dependson":param.bug_list}})

    return db.query("""
        SELECT blocked AS bug_id
            , CAST(null AS signed) AS modified_ts
            , CAST(null AS char(255)) AS modified_by
            , 'dependson' AS field_name
            , CAST(dependson AS SIGNED) AS new_value
            , CAST(null AS SIGNED) AS old_value
            , CAST(null AS signed) AS attach_id
            , 2 AS _merge_order
        FROM dependencies d
        WHERE
           {{blocks_filter}}
        UNION
        SELECT dependson dependson
            , null
            , null
            , 'blocked'
            , CAST(blocked AS SIGNED)
            , null
            , null
            , 2
        FROM dependencies d
        WHERE
            {{dependson_filter}}
        ORDER BY bug_id
    """, param)


def get_duplicates(db, param):
    param.dupe_filter=db.esfilter2sqlwhere({"terms":{"dupe":param.bug_list}})
    param.dupe_of_filter=db.esfilter2sqlwhere({"terms":{"dupe_of":param.bug_list}})

    return db.query("""
        SELECT dupe AS bug_id
            , CAST(null AS signed) AS modified_ts
            , CAST(null AS char(255)) AS modified_by
            , 'dupe_of' AS field_name
            , CAST(dupe_of AS SIGNED) AS new_value
            , CAST(null AS SIGNED) AS old_value
            , CAST(null AS signed) AS attach_id
            , 2 AS _merge_order
        FROM duplicates d
        WHERE
            {{dupe_filter}}
        UNION
        SELECT dupe_of
            , null
            , null
            , 'dupe_by'
            , CAST(dupe AS SIGNED)
            , null
            , null
            , 2
        FROM duplicates d
        WHERE
            {{dupe_of_filter}}
        ORDER BY bug_id
    """, param)


def get_bug_groups(db, param):
    param.bug_filter=db.esfilter2sqlwhere({"terms":{"bug_id":param.bug_list}})

    return db.query("""
        SELECT bug_id
            , CAST(null AS signed) AS modified_ts
            , CAST(null AS char(255)) AS modified_by
            , 'bug_group' AS field_name
            , lower(CAST(g.`name` AS char(255))) AS new_value
            , CAST(null AS char(255)) AS old_value
            , CAST(null AS signed) AS attach_id
            , 2 AS _merge_order
        FROM bug_group_map bg
        JOIN groups g ON bg.group_id = g.id
        WHERE
            {{bug_filter}}
    """, param)



def get_cc(db, param):
    param.bug_filter=db.esfilter2sqlwhere({"terms":{"bug_id":param.bug_list}})

    return db.query("""
        SELECT bug_id
            , CAST(null AS signed) AS modified_ts
            , CAST(null AS char(255)) AS modified_by
            , 'cc' AS field_name
            , lower(CAST(p.login_name AS char(255))) AS new_value
            , CAST(null AS char(255)) AS old_value
            , CAST(null AS signed) AS attach_id
            , 2 AS _merge_order
        FROM
            cc
        JOIN
            profiles p ON cc.who = p.userid
        WHERE
            {{bug_filter}}
    """, param)


def get_all_cc_changes(db, bug_list):
    CC_FIELD_ID = 37

    if not bug_list:
        return []

    return db.query("""
            SELECT
                bug_id,
                CAST({{max_time}} AS signed) AS modified_ts,
                CAST(null AS char(255)) AS new_value,
                lower(CAST(p.login_name AS CHAR(255) CHARACTER SET utf8)) AS old_value
            FROM
                cc
            LEFT JOIN
                profiles p ON cc.who = p.userid
            WHERE
                {{bug_filter}}
        UNION ALL
            SELECT
                a.bug_id,
                UNIX_TIMESTAMP(CONVERT_TZ(bug_when, 'US/Pacific','UTC'))*1000 AS modified_ts,
                lower(CAST(trim(added) AS CHAR CHARACTER SET utf8)) AS new_value,
                lower(CAST(trim(removed) AS CHAR CHARACTER SET utf8)) AS old_value
            FROM
                bugs_activity a
            WHERE
                a.fieldid = {{cc_field_id}} AND
                {{bug_filter}}
    """, {
        "max_time":MAX_TIME,
        "cc_field_id":CC_FIELD_ID,
        "bug_filter":db.esfilter2sqlwhere({"terms":{"bug_id":bug_list}})
    })



def get_keywords(db, param):
    param.bug_filter=db.esfilter2sqlwhere({"terms":{"bug_id":param.bug_list}})

    return db.query("""
        SELECT bug_id
            , NULL AS modified_ts
            , NULL AS modified_by
            , 'keywords' AS field_name
            , lower(kd.name) AS new_value
            , NULL AS old_value
            , NULL AS attach_id
            , 2 AS _merge_order
        FROM keywords k
        JOIN keyworddefs kd ON k.keywordid = kd.id
        WHERE
            {{bug_filter}}
        ORDER BY bug_id
    """, param)


def get_attachments(db, param):
    """
    GET ALL CURRENT ATTACHMENTS
    """
    if param.allow_private_bugs:
        param.attachments_filter=SQL("1=1")  #ALWAYS TRUE, ALLOWS ALL ATTACHMENTS
    else:
        param.attachments_filter=SQL("isprivate=0")

    param.bug_filter=db.esfilter2sqlwhere({"terms":{"bug_id":param.bug_list}})


    output=db.query("""
        SELECT bug_id
            , UNIX_TIMESTAMP(CONVERT_TZ(a.creation_ts, 'US/Pacific','UTC'))*1000 AS modified_ts
            , lower(login_name) AS modified_by
            , UNIX_TIMESTAMP(CONVERT_TZ(a.creation_ts, 'US/Pacific','UTC'))*1000 AS created_ts
            , login_name AS created_by
            , ispatch AS 'attachments_ispatch'
            , isobsolete AS 'attachments_isobsolete'
            , isprivate AS 'attachments_isprivate'
            , attach_id
        FROM
            attachments a
            JOIN profiles p ON a.submitter_id = p.userid
        WHERE
            {{bug_filter}} AND
            {{attachments_filter}}
        ORDER BY
            bug_id,
            attach_id,
            a.creation_ts
    """, param)
    return flatten_attachments(output)


attachments_fields = ["created_ts", "created_by", "attachments_ispatch", "attachments_isobsolete", "attachments_isprivate"]


def flatten_attachments(data):
    output=[]
    for r in data:
        for a in attachments_fields:
            output.append(Struct(
                bug_id=r.bug_id,
                modified_ts=r.modified_ts,
                modified_by=r.modified_by,
                field_name=a,
                new_value=r[a],  #THESE NAMES HAVE DOTS IN THEM
                attach_id=r.attach_id,
                _merge_order=7
            ))
    return output


def get_bug_see_also(db, param):
    param.bug_filter=db.esfilter2sqlwhere({"terms":{"bug_id":param.bug_list}})


    return db.query("""
        SELECT bug_id
            , CAST(null AS signed) AS modified_ts
            , CAST(null AS char(255)) AS modified_by
            , 'see_also' AS field_name
            , CAST(`value` AS char(255)) AS new_value
            , CAST(null AS char(255)) AS old_value
            , CAST(null AS signed) AS attach_id
            , 2 AS _merge_order
        FROM bug_see_also
        WHERE
            {{bug_filter}}
        ORDER BY bug_id
    """, param)



def get_new_activities(db, param):

    if param.allow_private_bugs:
        param.screened_fields=SQL(SCREENED_FIELDDEFS)
    else:
        param.screened_fields=SQL([-1])

    #TODO: CF_LAST_RESOLVED IS IN PDT, FIX IT
    param.bug_filter=db.esfilter2sqlwhere({"terms":{"a.bug_id":param.bug_list}})
    param.mixed_case_fields=SQL(MIXED_CASE)

    return db.query("""
        SELECT
            a.bug_id,
            UNIX_TIMESTAMP(CONVERT_TZ(bug_when, 'US/Pacific','UTC'))*1000 AS modified_ts,
            lower(login_name) AS modified_by,
            replace(field.`name`, '.', '_') AS field_name,
            CAST(
                CASE
                WHEN a.fieldid IN {{screened_fields}} THEN '<screened>'
                WHEN a.fieldid IN {{mixed_case_fields}} THEN trim(added)
                WHEN trim(added)='' THEN NULL
                ELSE lower(trim(added))
                END
            AS CHAR CHARACTER SET utf8) AS new_value,
            CAST(
                CASE
                WHEN a.fieldid IN {{screened_fields}} THEN '<screened>'
                WHEN a.fieldid IN {{mixed_case_fields}} THEN trim(removed)
                WHEN trim(removed)='' THEN NULL
                ELSE lower(trim(removed))
                END
            AS CHAR CHARACTER SET utf8) AS old_value,
            attach_id,
            9 AS _merge_order
        FROM
            bugs_activity a
        JOIN
            profiles p ON a.who = p.userid
        JOIN
            fielddefs field ON a.fieldid = field.`id`
        WHERE
            {{bug_filter}} AND
            bug_when >= {{start_time_str}}
        ORDER BY
            bug_id,
            bug_when DESC,
            attach_id
    """, param)


def get_flags(db, param):
    param.bug_filter=db.esfilter2sqlwhere({"terms":{"bug_id":param.bug_list}})

    return db.query("""
        SELECT bug_id
            , UNIX_TIMESTAMP(CONVERT_TZ(f.creation_date, 'US/Pacific','UTC'))*1000 AS modified_ts
            , ps.login_name AS modified_by
            , 'flagtypes_name' AS field_name
            , CONCAT(ft.`name`,status,IF(requestee_id IS NULL,'',CONCAT('(',pr.login_name,')'))) AS new_value
            , CAST(null AS char(255)) AS old_value
            , attach_id
            , 8 AS _merge_order
        FROM
            flags f
        JOIN `flagtypes` ft ON f.type_id = ft.id
        JOIN profiles ps ON f.setter_id = ps.userid
        LEFT JOIN profiles pr ON f.requestee_id = pr.userid
        WHERE
            {{bug_filter}}
        ORDER BY
            bug_id
    """, param)


def get_comments(db, param):
    if param.allow_private_bugs:
        return []
    if not param.bug_list:
        return []

    param.comments_filter=SQL("isprivate=0")
    param.bug_filter=db.esfilter2sqlwhere({"terms":{"bug_id":param.bug_list}})

    try:
        comments=db.query("""
            SELECT
                c.comment_id,
                c.bug_id,
                p.login_name modified_by,
                UNIX_TIMESTAMP(CONVERT_TZ(bug_when, 'US/Pacific','UTC'))*1000 AS modified_ts,
                c.thetext comment,
                c.isprivate
            FROM
                longdescs c
            LEFT JOIN
                profiles p ON c.who = p.userid
            WHERE
                {{bug_filter}} AND
                bug_when >= {{start_time_str}} AND
                {{comments_filter}}
            """, param)

        return comments
    except Exception, e:
        Log.error("can not get comment data", e)

def get_comments_by_id(db, comments, param):
    """
    GET SPECIFIC COMMENTS
    """
    if param.allow_private_bugs:
        return []

    param.comments_filter=db.esfilter2sqlwhere({"and":[
        {"term":{"isprivate":0}},
        {"terms":{"c.comment_id":comments}}
    ]})

    try:
        comments=db.query("""
            SELECT
                c.comment_id,
                c.bug_id,
                p.login_name modified_by,
                UNIX_TIMESTAMP(CONVERT_TZ(bug_when, 'US/Pacific','UTC'))*1000 AS modified_ts,
                c.thetext comment,
                c.isprivate
            FROM
                longdescs c
            LEFT JOIN
                profiles p ON c.who = p.userid
            WHERE
                {{comments_filter}}
            """, param)

        return comments
    except Exception, e:
        Log.error("can not get comment data", e)

