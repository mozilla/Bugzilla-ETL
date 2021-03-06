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

from jx_mysql import esfilter2sqlwhere
from jx_python import jx
from mo_dots import Data, wrap
from mo_logs import Log
from mo_threads import Lock
from mo_times.timer import Timer
from pyLibrary import convert
from pyLibrary.sql import SQL, sql_list, sql_alias, sql_iso, SQL_NEG_ONE
from pyLibrary.sql.mysql import quote_column, quote_value

# USING THE TEXT DATETIME OF EPOCH THROWS A WARNING!  USE ONE SECOND PAST EPOCH AS MINIMUM TIME.
MIN_TIMESTAMP = 1000  # MILLISECONDS SINCE EPOCH
MAX_TIMESTAMP = (10 * 1000 * 1000 * 1000 - 1) * 1000.0
GLOBAL_LOCK = Lock()

#ALL BUGS IN PRIVATE ETL HAVE SCREENED FIELDS
SCREENED_FIELDDEFS = [
    19,  # bug_file_loc
    24,  # short_desc
    42,  # longdesc
    45,  # attachments.description
    56,  # alias
    64,  # attachments.filename
    74,  # content
    83,  # attach_data.thedata
    496, # cf_user_story
]

# CERTAIN GROUPS IN PRIVATE ETL HAVE HAVE WHITEBOARD SCREENED
SCREENED_WHITEBOARD_BUG_GROUPS = [
    "legal",
    "consulting",
    "finance",
    "marketing-private",
    "partner-confidential",
    "qualcomm-confidential",
    "pr-private",
    "marketing private",
    "hr"
]

MIXED_CASE = [
    19, #bug_file_loc
    24  #short_desc
]

PRIVATE_ATTACHMENT_FIELD_ID = 65
PRIVATE_COMMENTS_FIELD_ID = 82
PRIVATE_BUG_GROUP_FIELD_ID = 66
STATUS_WHITEBOARD_FIELD_ID = 22

SCREENED_BUG_COLUMNS = [
    "bug_file_loc",
    "short_desc",
    "alias",
    "cf_user_story"
]

BUGS_COLUMNS = None
SCREENED_BUG_GROUP_IDS = None


def get_current_time(db):
    """
    RETURN GMT TIME
    """
    output = db.query("SELECT UNIX_TIMESTAMP(now()) `value`")[0].value
    if output == None:
        Log.error("I am guessing you did not add the timezone database!  See tests/resources/mySQL/README.md")
    return convert.unix2datetime(output)


def milli2string(db, value):
    """
    CONVERT GMT MILLI TO BUGZILLA DATETIME
    """
    value = max(value, MIN_TIMESTAMP)

    output = db.query(
        "SELECT CAST(FROM_UNIXTIME({{start_time}}/1000) AS CHAR) `value`",
        {"start_time": value}
    )[0].value

    if output[19]=='.':  #1970-01-01 00:00:00.0000
        output=output[:-1]
    else:
        Log.error("unexpected date format")
    return output


def get_screened_whiteboard(db):
    global SCREENED_BUG_GROUP_IDS

    if not SCREENED_BUG_GROUP_IDS:
        groups = db.query(
            "SELECT id FROM groups WHERE {{where}}",
            {"where": esfilter2sqlwhere({"terms": {"name": SCREENED_WHITEBOARD_BUG_GROUPS}})}
        )
        SCREENED_BUG_GROUP_IDS = jx.select(groups, "id")


def get_bugs_table_columns(db, schema_name):
    global BUGS_COLUMNS, SCREENED_BUG_COLUMNS

    with GLOBAL_LOCK:
        if not BUGS_COLUMNS:
            explicitly_used_columns = {
                'bug_id',               #EXPLICIT
                'creation_ts',          #EXPLICIT
                'reporter',             #EXPLICIT
                'assigned_to',          #EXPLICIT
                'qa_contact',           #EXPLICIT
                'product_id',           #EXPLICIT
                'component_id',         #EXPLICIT
                'short_desc',           #EXPLICIT
                'bug_file_loc',         #EXPLICIT
                'status_whiteboard',    #EXPLICIT
                'deadline',             #NOT NEEDED
                'estimated_time',       #NOT NEEDED
                'delta_ts',             #NOT NEEDED
                'lastdiffed',           #NOT NEEDED
                'cclist_accessible',    #NOT NEEDED
                'reporter_accessible'   #NOT NEEDED
            } - set(SCREENED_BUG_COLUMNS)

            all_columns = db.query(
                "SELECT column_name, column_type FROM information_schema.columns WHERE " +
                esfilter2sqlwhere({"and": [
                    {"eq": {"table_schema": schema_name}},
                    {"eq": {"table_name": "bugs"}},
                    {"not": {"in": {"column_name": explicitly_used_columns}}}
                ]})
            )

            BUGS_COLUMNS = wrap([c for c in all_columns if c.column_name not in SCREENED_BUG_COLUMNS])
            SCREENED_BUG_COLUMNS = wrap([c for c in all_columns if c.column_name in SCREENED_BUG_COLUMNS])

def get_private_bugs_for_delete(db, param):
    if param.allow_private_bugs:
        return {0}  # NO BUGS TO DELETE

    try:
        with Timer("get all private bug ids"):
            private_bugs = db.query("SELECT DISTINCT bug_id FROM bug_group_map")
            return set(private_bugs.bug_id) | {0}
    except Exception as e:
        Log.error("problem getting private bugs", e)


def get_recent_private_bugs(db, param):
    """
    GET ONLY BUGS THAT HAVE SWITCHED PRIVACY INDICATOR
    THIS LIST IS USED TO SIGNAL BUGS THAT NEED TOTAL RE-ETL
    """
    param.field_id = PRIVATE_BUG_GROUP_FIELD_ID

    try:
        output = db.query("""
        SELECT
            a.bug_id
        FROM
            bugs_activity a
        WHERE
            bug_when >= {{start_time_str}} AND
            fieldid={{field_id}}
        """, param)

        return set(output.bug_id)

    except Exception as e:
        Log.error("problem getting recent private attachments", e)


def get_recent_private_attachments(db, param):
    """
    GET ONLY RECENT ATTACHMENTS THAT HAVE SWITCHED PRIVACY INDICATOR
    THIS LIST IS USED TO SIGNAL BUGS THAT NEED TOTAL RE-ETL
    """
    if param.allow_private_bugs:
        return []

    param.field_id = PRIVATE_ATTACHMENT_FIELD_ID

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
    except Exception as e:
        Log.error("problem getting recent private attachments", e)


def get_recent_private_comments(db, param):
    """
    GET COMMENTS THAT HAVE HAD THEIR PRIVACY INDICATOR CHANGED
    """
    if param.allow_private_bugs:
        return []

    param.field_id = PRIVATE_COMMENTS_FIELD_ID

    try:
        comments = db.query("""
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
    except Exception as e:
        Log.error("problem getting recent private attachments", e)


def get_bugs(db, param):
    try:
        get_bugs_table_columns(db, db.settings.schema)
        get_screened_whiteboard(db)

        param.bugs_columns = BUGS_COLUMNS.column_name
        param.bugs_columns_SQL = sql_list(quote_column(c.column_name, "b") for c in BUGS_COLUMNS)
        param.screened_whiteboard = esfilter2sqlwhere({"and": [
            {"exists": "bgm.bug_id"},
            {"terms": {"bgm.group_id": SCREENED_BUG_GROUP_IDS}}
        ]})
        param.allowed_bugs = esfilter2sqlwhere({"terms": {"bgm.bug_id": param.bug_list}})

        if param.allow_private_bugs:
            param.bug_filter = esfilter2sqlwhere({"and": [
                {"terms": {"b.bug_id": param.bug_list}}
            ]})
            param.sensitive_columns = sql_list(
                sql_alias(quote_value('[screened]'), quote_column(c.column_name))
                for c in SCREENED_BUG_COLUMNS
            )
        else:
            param.bug_filter = esfilter2sqlwhere({"and": [
                {"terms": {"b.bug_id": param.bug_list}},
                {"missing": "bgm.bug_id"}
            ]})
            param.sensitive_columns = sql_list(
                quote_column(c.column_name, "b")
                for c in SCREENED_BUG_COLUMNS
            )

        bugs = db.query(
            """
            SELECT
                b.bug_id,
                UNIX_TIMESTAMP(b.creation_ts)*1000 AS modified_ts,
                pr.login_name AS modified_by,
                UNIX_TIMESTAMP(b.creation_ts)*1000 AS created_ts,
                pr.login_name AS created_by,
                pa.login_name AS assigned_to,
                pq.login_name AS qa_contact,
                prod.`name` AS product,
                comp.`name` AS component,
                CASE
                WHEN bgm.screened AND b.status_whiteboard IS NOT NULL AND trim(b.status_whiteboard)<>''
                THEN '[screened]'
                ELSE b.status_whiteboard
                END status_whiteboard,
                {{sensitive_columns}},
                {{bugs_columns_SQL}}
            FROM
                bugs b
            LEFT JOIN
                profiles pr ON b.reporter = pr.userid
            LEFT JOIN
                profiles pa ON b.assigned_to = pa.userid
            LEFT JOIN
                profiles pq ON b.qa_contact = pq.userid
            LEFT JOIN
                products prod ON prod.id = product_id
            LEFT JOIN
                components comp ON comp.id = component_id
            LEFT JOIN
                (  # ALLOW ONLY ONE GROUP
                    SELECT 
                        bug_id, 
                        MAX(CASE WHEN {{screened_whiteboard}} THEN 1 ELSE 0 END) screened
                    FROM 
                        bug_group_map bgm 
                    WHERE 
                        {{allowed_bugs}} 
                    GROUP BY 
                        bug_id
                ) bgm ON bgm.bug_id = b.bug_id
            WHERE
                {{bug_filter}}
            """,
            param
        )

        if len(bugs) > len(param.bug_list):
            Log.error("expecting {{num}} bugs; likely a logic error", num=len(param.bug_list))
        #bugs IS LIST OF BUGS WHICH MUST BE CONVERTED TO THE DELTA RECORDS FOR ALL FIELDS
        output = []
        for r in bugs:
            flatten_bugs_record(r, output)

        return output
    except Exception as e:
        Log.error("can not get basic bug data", cause=e)


def flatten_bugs_record(r, output):
    for field_name, value in r.items():
        if value != "---":
            newRow = Data()
            newRow.bug_id = r.bug_id
            newRow.modified_ts = r.modified_ts
            newRow.modified_by = r.modified_by
            newRow.field_name = field_name
            newRow.new_value = value
            newRow._merge_order = 1
            output.append(newRow)


def get_dependencies(db, param):
    param.blocks_filter = esfilter2sqlwhere({"terms": {"blocked": param.bug_list}})
    param.dependson_filter = esfilter2sqlwhere({"terms": {"dependson": param.bug_list}})

    return db.query("""
        SELECT blocked AS bug_id
            , CAST(null AS signed) AS modified_ts
            , CAST(null AS CHAR) AS modified_by
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
    param.dupe_filter = esfilter2sqlwhere({"terms": {"dupe": param.bug_list}})
    param.dupe_of_filter = esfilter2sqlwhere({"terms": {"dupe_of": param.bug_list}})

    return db.query("""
        SELECT dupe AS bug_id
            , CAST(null AS signed) AS modified_ts
            , CAST(null AS CHAR) AS modified_by
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
    param.bug_filter = esfilter2sqlwhere({"terms": {"bug_id": param.bug_list}})

    return db.query("""
        SELECT bug_id
            , CAST(null AS signed) AS modified_ts
            , CAST(null AS CHAR) AS modified_by
            , 'bug_group' AS field_name
            , CAST(g.`name` AS CHAR) AS new_value
            , CAST(null AS CHAR) AS old_value
            , CAST(null AS signed) AS attach_id
            , 2 AS _merge_order
        FROM bug_group_map bg
        JOIN groups g ON bg.group_id = g.id
        WHERE
            {{bug_filter}}
    """, param)


def get_cc(db, param):
    param.bug_filter = esfilter2sqlwhere({"terms": {"bug_id": param.bug_list}})

    return db.query("""
        SELECT bug_id
            , CAST(null AS signed) AS modified_ts
            , CAST(null AS CHAR) AS modified_by
            , 'cc' AS field_name
            , CAST(p.login_name AS CHAR) AS new_value
            , CAST(null AS CHAR) AS old_value
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

    return db.query(
        """
            SELECT
                bug_id,
                CAST({{max_time}} AS signed) AS modified_ts,
                CAST(null AS CHAR) AS new_value,
                CAST(p.login_name AS CHAR CHARACTER SET utf8) AS old_value
            FROM
                cc
            LEFT JOIN
                profiles p ON cc.who = p.userid
            WHERE
                {{bug_filter}}
        UNION ALL
            SELECT
                a.bug_id,
                UNIX_TIMESTAMP(bug_when)*1000 AS modified_ts,
                CAST(trim(added) AS CHAR CHARACTER SET utf8) AS new_value,
                CAST(trim(removed) AS CHAR CHARACTER SET utf8) AS old_value
            FROM
                bugs_activity a
            WHERE
                a.fieldid = {{cc_field_id}} AND
                {{bug_filter}}
        """,
        {
            "max_time": MAX_TIMESTAMP,
            "cc_field_id": CC_FIELD_ID,
            "bug_filter": esfilter2sqlwhere({"terms": {"bug_id": bug_list}})
        },
        stream=True
    )


def get_tracking_flags(db, param):
    param.bug_filter = esfilter2sqlwhere({"terms": {"bug_id": param.bug_list}})

    return db.query("""
        SELECT
            bug_id,
            CAST({{start_time}} AS signed) AS modified_ts,
            f.name AS field_name,
            t.value AS new_value,
            1 AS _merge_order
        FROM
            tracking_flags_bugs t
        JOIN
            tracking_flags f on f.id=t.tracking_flag_id
        WHERE
            {{bug_filter}}
        ORDER BY
            bug_id
    """, param)


def get_keywords(db, param):
    param.bug_filter = esfilter2sqlwhere({"terms": {"bug_id": param.bug_list}})

    return db.query("""
        SELECT bug_id
            , NULL AS modified_ts
            , NULL AS modified_by
            , 'keywords' AS field_name
            , kd.name AS new_value
            , NULL AS old_value
            , NULL AS attach_id
            , 2 AS _merge_order
        FROM keywords k
        JOIN keyworddefs kd ON k.keywordid = kd.id
        WHERE
            {{bug_filter}}
        ORDER BY bug_id
    """, param)


def get_tags(db, param):
    param.bug_filter = esfilter2sqlwhere({"terms": {"bug_id": param.bug_list}})

    return db.query(
        """
        SELECT
            bug_id,
            NULL AS modified_ts,
            NULL AS modified_by,
            'tags' AS field_name,
            tag.name as new_value,
            NULL AS old_value,
            NULL AS attach_id,
            2 AS _merge_order
        FROM
            bug_tag b
        LEFT JOIN
            tag on tag.id = b.tag_id
        WHERE
            {{bug_filter}}
        ORDER BY
            bug_id
        """,
        param
    )


def get_attachments(db, param):
    """
    GET ALL CURRENT ATTACHMENTS
    """
    if param.allow_private_bugs:
        param.attachments_filter = SQL("1=1")  #ALWAYS TRUE, ALLOWS ALL ATTACHMENTS
    else:
        param.attachments_filter = SQL("isprivate=0")

    param.bug_filter = esfilter2sqlwhere({"terms": {"bug_id": param.bug_list}})

    output = db.query("""
        SELECT bug_id
            , UNIX_TIMESTAMP(a.creation_ts)*1000 AS modified_ts
            , login_name AS modified_by
            , UNIX_TIMESTAMP(a.creation_ts)*1000 AS created_ts
            , login_name AS created_by
            , ispatch AS 'attachments_ispatch'
            , isobsolete AS 'attachments_isobsolete'
            , isprivate AS 'attachments_isprivate'
            , mimetype AS 'attachments_mimetype'
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


def flatten_attachments(data):
    output = []
    for r in data:
        for k,v in r.items():
            if k=="bug_id":
                continue
            output.append(Data(
                bug_id=r.bug_id,
                modified_ts=r.modified_ts,
                modified_by=r.modified_by,
                field_name=k,
                new_value=v, #THESE NAMES HAVE DOTS IN THEM
                attach_id=r.attach_id,
                _merge_order=7
            ))
    return output


def get_bug_see_also(db, param):
    param.bug_filter = esfilter2sqlwhere({"terms": {"bug_id": param.bug_list}})

    return db.query("""
        SELECT bug_id
            , CAST(null AS signed) AS modified_ts
            , CAST(null AS CHAR) AS modified_by
            , 'see_also' AS field_name
            , CAST(`value` AS CHAR) AS new_value
            , CAST(null AS CHAR) AS old_value
            , CAST(null AS signed) AS attach_id
            , 2 AS _merge_order
        FROM bug_see_also
        WHERE
            {{bug_filter}}
        ORDER BY bug_id
    """, param)


def get_new_activities(db, param):
    get_screened_whiteboard(db)

    if param.allow_private_bugs:
        param.screened_fields = sql_iso(sql_list(map(quote_value, SCREENED_FIELDDEFS)))
    else:
        param.screened_fields = sql_iso(SQL_NEG_ONE)

    param.bug_filter = esfilter2sqlwhere({"terms": {"a.bug_id": param.bug_list}})
    param.mixed_case_fields = sql_iso(sql_list(map(quote_value, MIXED_CASE)))
    param.screened_whiteboard = esfilter2sqlwhere({"terms": {"m.group_id": SCREENED_BUG_GROUP_IDS}})
    param.whiteboard_field = STATUS_WHITEBOARD_FIELD_ID

    output = db.query("""
        SELECT
            a.id,
            a.bug_id,
            UNIX_TIMESTAMP(bug_when)*1000 AS modified_ts,
            p.login_name AS modified_by,
            replace(field.`name`, '.', '_') AS field_name,
            CAST(
                CASE
                WHEN a.fieldid IN {{screened_fields}} THEN '[screened]'
                WHEN m.bug_id IS NOT NULL AND a.fieldid={{whiteboard_field}} AND added IS NOT NULL AND trim(added)<>'' THEN '[screened]'
                WHEN a.fieldid IN {{mixed_case_fields}} THEN added
                WHEN trim(added)='' THEN NULL
                # WHEN new_qa_contact.userid IS NOT NULL THEN new_qa_contact.login_name
                ELSE added
                END
            AS CHAR CHARACTER SET utf8) AS new_value,
            CAST(
                CASE
                WHEN a.fieldid IN {{screened_fields}} THEN '[screened]'
                WHEN m.bug_id IS NOT NULL AND a.fieldid={{whiteboard_field}} AND removed IS NOT NULL AND trim(removed)<>'' THEN '[screened]'
                WHEN a.fieldid IN {{mixed_case_fields}} THEN removed
                WHEN trim(removed)='' THEN NULL
                # WHEN old_qa_contact.userid IS NOT NULL THEN old_qa_contact.login_name
                ELSE removed
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
        LEFT JOIN
            bug_group_map m on m.bug_id=a.bug_id AND {{screened_whiteboard}}
        # LEFT JOIN
        #     profiles new_qa_contact
        # ON
        #     new_qa_contact.userid=
        #         CASE
        #         WHEN a.fieldid <> 36 THEN -1
        #         WHEN NOT a.added REGEXP '^[0-9]+$' THEN -1
        #         ELSE CAST(a.added AS UNSIGNED)
        #         END
        # LEFT JOIN
        #     profiles old_qa_contact
        # ON
        #   old_qa_contact.userid=
        #       CASE
        #       WHEN a.fieldid <> 36 THEN -1
        #       WHEN NOT a.removed REGEXP '^[0-9]+$' THEN -1
        #       ELSE CAST(a.removed AS UNSIGNED)
        #       END
        WHERE
            {{bug_filter}}
            # NEED TO QUERY ES TO GET bug_version_num OTHERWISE WE NEED ALL HISTORY
            # AND bug_when >= {{start_time_str}}
        ORDER BY
            a.bug_id,
            bug_when DESC,
            attach_id
    """, param)

    return output


def get_flags(db, param):
    param.bug_filter = esfilter2sqlwhere({"terms": {"bug_id": param.bug_list}})

    return db.query("""
        SELECT
            bug_id,
            UNIX_TIMESTAMP(f.creation_date)*1000 AS modified_ts,
            ps.login_name AS modified_by,
            'flagtypes_name' AS field_name,
            CONCAT(
                ft.`name`,
                status,
                CASE
                WHEN f.requestee_id IS NULL THEN ''
                ELSE CONCAT('(', pr.login_name, ')')
                END
            ) AS new_value,
            CAST(null AS CHAR) AS old_value,
            attach_id,
            8 AS _merge_order
        FROM
            flags f
        JOIN
            flagtypes ft ON f.type_id = ft.id
        JOIN
            profiles ps ON f.setter_id = ps.userid
        LEFT JOIN
            profiles pr ON f.requestee_id = pr.userid
        WHERE
            {{bug_filter}}
        ORDER BY
            bug_id
    """, param)


def get_comments(db, param):
    if not param.bug_list:
        return []

    if param.allow_private_bugs:
        param.comment_field = SQL("'[screened]' comment")
        param.bug_filter = esfilter2sqlwhere({"and": [
            {"terms": {"bug_id": param.bug_list}}
        ]})
    else:
        param.comment_field = SQL("c.thetext comment")
        param.bug_filter = esfilter2sqlwhere({"and": [
            {"terms": {"bug_id": param.bug_list}},
            {"term": {"isprivate": 0}}
        ]})

    try:
        comments = db.query("""
            SELECT
                c.comment_id,
                c.bug_id,
                p.login_name modified_by,
                UNIX_TIMESTAMP(bug_when)*1000 AS modified_ts,
                {{comment_field}},
                c.isprivate
            FROM
                longdescs c
            LEFT JOIN
                profiles p ON c.who = p.userid
            LEFT JOIN
                longdescs_tags t ON t.comment_id=c.comment_id AND t.tag <> 'deleted'
            WHERE
                {{bug_filter}} AND
                bug_when >= {{start_time_str}}
            """, param)

        return comments
    except Exception as e:
        Log.error("can not get comment data", e)


def get_comments_by_id(db, comments, param):
    """
    GET SPECIFIC COMMENTS
    """
    if param.allow_private_bugs:
        return []

    param.comments_filter = esfilter2sqlwhere({"and": [
        {"term": {"isprivate": 0}},
        {"terms": {"c.comment_id": comments}}
    ]})

    try:
        comments = db.query("""
            SELECT
                c.comment_id,
                c.bug_id,
                p.login_name modified_by,
                UNIX_TIMESTAMP(bug_when)*1000 AS modified_ts,
                c.thetext comment,
                c.isprivate
            FROM
                longdescs c
            LEFT JOIN
                profiles p ON c.who = p.userid
            LEFT JOIN
                longdescs_tags t ON t.comment_id=c.comment_id AND t.tag <> 'deleted'
            WHERE
                {{comments_filter}}
            """, param)

        return comments
    except Exception as e:
        Log.error("can not get comment data", e)

