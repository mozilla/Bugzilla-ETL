# encoding: utf-8
#
from datetime import datetime
from bzETL.extract_bugzilla import milli2string, get_current_time
from bzETL.util.cnv import CNV
from bzETL.util.db import DB
from bzETL.util.logs import Log
from bzETL.util.struct import Struct
from bzETL.util.timer import Timer


def make_test_instance(db_settings):
    if not db_settings.filename:
        Log.note("Database schema will not be touched")
        return

    with Timer("Make database instance"):
        try:
            #CLEAR SCHEMA
            Log.note("Make empty {{schema}} schema", {"schema":db_settings.schema})
            no_schema=db_settings.copy()
            no_schema.schema = None
            with DB(no_schema) as db:
                db.execute("DROP DATABASE IF EXISTS {{schema}}", {"schema":db.quote_column(db_settings.schema)})
                db.execute("CREATE DATABASE {{schema}}", {"schema":db.quote_column(db_settings.schema)})

            #FILL SCHEMA
            Log.note("Fill {{schema}} schema with data", {"schema":db_settings.schema})
            DB.execute_file(db_settings, db_settings.filename)

            #FIX BECAUSE DATETIME ARE ALL IN EDT (MY MISTAKE)
            with DB(db_settings) as db:
                db.execute("UPDATE attachments SET creation_ts=CONVERT_TZ(creation_ts, 'UTC', 'US/Eastern')")
                db.execute("UPDATE attachments SET modification_time=CONVERT_TZ(modification_time, 'UTC', 'US/Eastern')")
                db.execute("UPDATE bugs SET cf_due_date=CONVERT_TZ(cf_due_date, 'UTC', 'US/Eastern')")
                db.execute("UPDATE bugs SET cf_last_resolved=CONVERT_TZ(cf_last_resolved, 'UTC', 'US/Eastern')")
                db.execute("UPDATE bugs SET creation_ts=CONVERT_TZ(creation_ts, 'UTC', 'US/Eastern')")
                db.execute("UPDATE bugs SET deadline=CONVERT_TZ(deadline, 'UTC', 'US/Eastern')")
                db.execute("UPDATE bugs SET delta_ts=CONVERT_TZ(delta_ts, 'UTC', 'US/Eastern')")
                db.execute("UPDATE bugs SET lastdiffed=CONVERT_TZ(lastdiffed, 'UTC', 'US/Eastern')")
                db.execute("UPDATE bugs_activity SET bug_when=CONVERT_TZ(bug_when, 'UTC', 'US/Eastern')")
                db.execute("UPDATE flags SET creation_date=CONVERT_TZ(creation_date, 'UTC', 'US/Eastern')")
                db.execute("UPDATE flags SET modification_date=CONVERT_TZ(modification_date, 'UTC', 'US/Eastern')")
                db.execute("UPDATE longdescs SET bug_when=CONVERT_TZ(bug_when, 'UTC', 'US/Eastern')")
                db.execute("UPDATE profiles SET creation_ts=CONVERT_TZ(creation_ts, 'UTC', 'US/Eastern')")
                db.execute("UPDATE profiles_activity SET profiles_when=CONVERT_TZ(profiles_when, 'UTC', 'US/Eastern')")


        except Exception, e:
            Log.error("Can not setup test database", e)


def mark_attachment_private(db, attach_id, isprivate):
    old_attach=db.query("SELECT * FROM attachments WHERE attach_id={{id}}", {"id":attach_id})[0]
    new_attach=old_attach.copy()
    new_attach.isprivate=isprivate

    diff(db, "attachments", old_attach, new_attach)
    db.update("attachments", {"attach_id":old_attach.attach_id}, new_attach)


def mark_comment_private(db, comment_id, isprivate):
    old_comment=db.query("SELECT * FROM longdescs WHERE comment_id={{id}}", {"id":comment_id})[0]
    new_comment=old_comment.copy()
    new_comment.isprivate=isprivate

    diff(db, "longdescs", old_comment, new_comment)
    db.update("longdescs", {"comment_id":old_comment.comment_id}, new_comment)


def add_bug_group(db, bug_id, group_name):
    group_exists=db.query("SELECT id FROM groups WHERE name={{name}}", {"name": group_name})
    if not group_exists:
        db.insert("groups", {
            "name":group_name,
            "description":group_name,
            "isbuggroup":1,
            "userregexp":0
        })
        group_exists=db.query("SELECT id FROM groups WHERE name={{name}}", {"name": group_name})
    group_id=group_exists[0].id

    diff(db, "bugs",
        Struct(bug_id=bug_id, bug_group=None),
        Struct(bug_id=bug_id, bug_group=group_name)
    )
    db.insert("bug_group_map", {"bug_id":bug_id, "group_id":group_id})


def remove_bug_group(db, bug_id, group_name):
    group_id=db.query("SELECT id FROM groups WHERE name={{name}}", {"name": group_name})[0].id

    diff(db, "bugs",
        Struct(bug_id=bug_id, bug_group=group_name),
        Struct(bug_id=bug_id, bug_group=None)
    )
    db.execute("DELETE FROM bug_group_map WHERE bug_id={{bug_id}} and group_id={{group_id}}", {
        "bug_id":bug_id,
        "group_id":group_id
    })




def diff(db, table, old_record, new_record):
    """
    UPDATE bugs_activity WITH THE CHANGES IN RECORDS
    """
    now = milli2string(db, CNV.datetime2milli(get_current_time(db)))
    changed = set(old_record.keys()) ^ set(new_record.keys())
    changed |= set([k for k, v in old_record.items() if v != new_record[k]])

    if table != u"bugs":
        prefix = table + u"."
    else:
        prefix = u""

    for c in changed:
        activity = Struct(
            bug_id=old_record.bug_id,
            who=1,
            bug_when=now,
            fieldid=db.query("SELECT id FROM fielddefs WHERE name={{field_name}}", {"field_name": prefix + c})[0].id,
            removed=old_record[c],
            added=new_record[c],
            attach_id=old_record.attach_id,
            comment_id=old_record.comment_id
        )
        db.insert("bugs_activity", activity)

        # db.update(table, old_value, new_value)

