from datetime import datetime
from bzETL.util.db import DB
from bzETL.util.logs import Log
from bzETL.util.struct import Struct


def make_test_instance(db_settings):
    try:
        #CLEAR SCHEMA
        no_schema=db_settings.copy()
        no_schema.schema=None
        with DB(no_schema) as db:
            db.execute("DROP DATABASE IF EXISTS {{schema}}", {"schema":db.quote_column(db_settings.schema)})
            db.execute("CREATE DATABASE {{schema}}", {"schema":db.quote_column(db_settings.schema)})

        #FILL SCHEMA
        DB.execute_file(db_settings, db_settings.filename)
    except Exception, e:
        Log.error("Can not setup test database", e)


def mark_attachment_private(db, attach_id):
    old_attach=db.query("SELECT * FROM attachments WHERE attach_id={{id}}", {"id":attach_id})
    new_attach=old_attach.copy()
    new_attach.isprivate=1

    diff(db, "attachments", old_attach, new_attach)
    db.update("attachments", old_attach, new_attach)


def mark_comment_private(db, comment_id):
    old_comment=db.query("SELECT * FROM longdescs WHERE comment_id={{id}}", {"id":comment_id})
    new_comment=old_comment.copy()
    new_comment.isprivate=1

    diff(db, "longdescs", old_comment, new_comment)
    db.update("longdescs", old_comment, new_comment)


def add_bug_group(db, bug_id, group_name):
    group_exists=db.query("SELECT id FROM groups WHERE name={{name}}", {"name": group_name})
    if len(group_exists)==0:
        db.insert("groups", {"name":group_name})
        group_exists=len(db.query("SELECT id FROM groups WHERE name={{name}}", {"name": group_name}))
    group_id=group_exists[0].id

    db.insert("bug_group_map", {"bug_id":bug_id, "group_id":group_id})
    diff(db, "bugs",
        Struct(bug_id=bug_id, bug_group=None),
        Struct(bug_id=bug_id, bug_group=group_name)
    )






def diff(db, table, old_value, new_value):
    changed=old_value.keys()^new_value.keys()
    changed+=set([k for k,v in old_value.items() if v!=new_value[k]])

    if table!=u"bugs":
        prefix=table+u"."
    else:
        prefix=u""

    for c in changed:
        activity=Struct(
            bug_id=old_value.bug_id,
            who=1,
            bug_when=datetime.utcnow(),
            fieldid=db.query("SELECT id FROM fielddefs WHERE name={{field_name}}", {"field_name":prefix+c})[0].id,
            removed=old_value[c],
            added=new_value[c],
            attach_id=old_value.attach_id
        )
        db.insert("bugs_activity", activity)

    db.update(table, old_value, new_value)

