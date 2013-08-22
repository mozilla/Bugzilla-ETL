from util.debug import D
from util.struct import Struct

def get_bugs(db, param):
    try:
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
                , ${BUGS_TABLE_COLUMNS_SQL}
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
        output=[]
        for r in bugs:
            flatten_bugs_record(r, param.BUGS_TABLE_COLUMNS, output)

        return output
    except Exception, e:
        D.error("can not get basic bug data", e)


def flatten_bugs_record(r, bugs_fields, output):
    newRow=Struct()
    newRow.bug_id=r.bug_id
    newRow.modified_ts=r.modified_ts
    newRow.modified_by=r.modified_by
    newRow.field_name="created_ts"
    newRow.field_value=r.created_ts
    newRow._merge_order=1
    output.append(newRow)

    newRow=Struct()
    newRow.bug_id=r.bug_id
    newRow.modified_ts=r.modified_ts
    newRow.modified_by=r.modified_by
    newRow.field_name="created_by"
    newRow.field_value=r.created_by
    newRow._merge_order=1
    output.append(newRow)

    newRow=Struct()
    newRow.bug_id=r.bug_id
    newRow.modified_ts=r.modified_ts
    newRow.modified_by=r.modified_by
    newRow.field_name="assigned_to"
    newRow.field_value=r.assigned_to
    newRow._merge_order=1
    output.append(newRow)

    newRow=Struct()
    newRow.bug_id=r.bug_id
    newRow.modified_ts=r.modified_ts
    newRow.modified_by=r.modified_by
    newRow.field_name="qa_contact"
    newRow.field_value=r.qa_contact
    newRow._merge_order=1
    output.append(newRow)

    newRow=Struct()
    newRow.bug_id=r.bug_id
    newRow.modified_ts=r.modified_ts
    newRow.modified_by=r.modified_by
    newRow.field_name="product"
    newRow.field_value=r.product
    newRow._merge_order=1
    output.append(newRow)

    newRow=Struct()
    newRow.bug_id=r.bug_id
    newRow.modified_ts=r.modified_ts
    newRow.modified_by=r.modified_by
    newRow.field_name="component"
    newRow.field_value=r.component
    newRow._merge_order=1
    output.append(newRow)

    for field_name in bugs_fields:
        value = r[field_name]
        if value != "---":
            newRow=Struct()
            newRow.bug_id=r.bug_id
            newRow.modified_ts=r.modified_ts
            newRow.modified_by=r.modified_by
            newRow.field_name=field_name
            newRow.field_value=value
            newRow._merge_order=1
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
            , NULL AS modified_ts
            , NULL AS modified_by
            , 'keywords' AS field_name
            , kd.name AS field_value
            , NULL AS field_value_removed
            , NULL AS attach_id
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
    output=db.query("""
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
    return flatten_attachments(output)

attachments_fields = ["created_ts", "created_by", "attachments.ispatch", "attachments.isobsolete", "attachments.isprivate"]

def flatten_attachments(data):
    output=[]
    for r in data:
        for a in attachments_fields:
            output.append(Struct(
                bug_id=r.bug_id,
                modified_ts=r.modified_ts,
                modified_by=r.modified_by,
                field_name=a,
                field_value=r.dict[a],  #THESE NAMES HAVE DOTS IN THEM
                attach_id=r.attach_id,
                _merge_order=7
            ))
    return output


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
            , CAST(CASE WHEN trim(added)='' THEN NULL ELSE trim(added) END AS CHAR CHARACTER SET utf8)   AS field_value
            , CAST(CASE WHEN trim(removed)='' THEN NULL ELSE trim(removed) END AS CHAR CHARACTER SET utf8)   AS field_value_removed
            , attach_id
            , 9 AS _merge_order
        FROM
            sanitized_bugs_activity a
        INNER JOIN (
            SELECT bug_id FROM bugs WHERE
            UNIX_TIMESTAMP(CONVERT_TZ(delta_ts, 'US/Pacific','UTC'))*1000 > ${START_TIME}
            AND ${BUG_IDS_PARTITION}
        ) b ON a.bug_id = b.bug_id
        JOIN
            fielddefs field ON a.fieldid = field.`id`
        JOIN
            profiles p ON a.who = p.userid
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
  