# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from bugzilla_etl.extract_bugzilla import milli2string, get_current_time
from mo_dots.datas import Data
from mo_files import File
from mo_logs import Log
from mo_times.timer import Timer
from pyLibrary import convert
from jx_mysql import esfilter2sqlwhere
from pyLibrary.sql.mysql import MySQL, quote_column, execute_file


def make_test_instance(db_settings):
    if db_settings.filename == None:
        Log.note("Database schema will not be touched")
        return

    with Timer("Make database instance"):
        try:
            #CLEAR SCHEMA
            Log.note("Make empty {{schema}} schema", schema=db_settings.schema)
            no_schema=db_settings.copy()
            no_schema.schema = None
            with MySQL(debug=False, kwargs=no_schema) as db:
                db.execute("DROP DATABASE IF EXISTS {{schema}}", {"schema": quote_column(db_settings.schema)})
                db.execute("CREATE DATABASE {{schema}}", {"schema": quote_column(db_settings.schema)})

                db.execute("USE {{schema}}", {"schema": quote_column(db_settings.schema)})
                file = File(db_settings.filename)
                if file.extension == 'zip':
                    sql = file.read_zipfile()
                else:
                    sql = File(db_settings.filename).read()

                for c in sql.split(";\r\n"):
                    db.execute(c)

        except Exception as e:
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
        Data(bug_id=bug_id, bug_group=None),
        Data(bug_id=bug_id, bug_group=group_name)
    )
    db.insert("bug_group_map", {"bug_id":bug_id, "group_id":group_id})


def remove_bug_group(db, bug_id, group_name):
    group_id=db.query("SELECT id FROM groups WHERE name={{name}}", {"name": group_name})[0].id

    diff(db, "bugs",
        Data(bug_id=bug_id, bug_group=group_name),
        Data(bug_id=bug_id, bug_group=None)
    )
    db.execute("DELETE FROM bug_group_map WHERE bug_id={{bug_id}} and group_id={{group_id}}", {
        "bug_id":bug_id,
        "group_id":group_id
    })




def diff(db, table, old_record, new_record):
    """
    UPDATE bugs_activity WITH THE CHANGES IN RECORDS
    """
    now = milli2string(db, convert.datetime2milli(get_current_time(db)))
    changed = set(old_record.keys()) ^ set(new_record.keys())
    changed |= set([k for k, v in old_record.items() if v != new_record[k]])

    if table != u"bugs":
        prefix = table + u"."
    else:
        prefix = u""

    for c in changed:
        fieldid=db.query("SELECT id FROM fielddefs WHERE name={{field_name}}", {"field_name": prefix + c})[0].id

        if fieldid == None:
            Log.error("Expecting a valid field name")

        activity = Data(
            bug_id=old_record.bug_id,
            who=1,
            bug_when=now,
            fieldid=fieldid,
            removed=old_record[c],
            added=new_record[c],
            attach_id=old_record.attach_id,
            comment_id=old_record.comment_id
        )
        db.insert("bugs_activity", activity)

    db.execute("UPDATE bugs SET delta_ts={{now}} WHERE {{where}}", {
        "now":now,
        "where":esfilter2sqlwhere({"term":{"bug_id":old_record.bug_id}})
    })

