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
from mo_dots import coalesce
from mo_logs import startup, constants, Log
from mo_threads import Process
from pyLibrary.sql import SQL_WHERE, SQL_SELECT, SQL_FROM, SQL_UNION, SQL
from pyLibrary.sql.mysql import MySQL, quote_sql, quote_list
from pyLibrary.sql.sqlite import quote_column


def bugzilla_extract(config):
    # extract schema with mysqldump
    # mysqldump -u root -p --no-data dbname > schema.sql
    # p = Process(
    #     "extract Bugzilla schema only",
    #     [
    #         config.mysqldump,
    #         "-u", config.source.username,
    #         "-p" + config.source.password,
    #         "-P", coalesce(config.source.port, 3306),
    #         "-h", config.source.host,
    #         "--no-data", config.source.schema,
    #         "--skip-lock-tables",
    #         ">", config.schema_file
    #     ],
    #     shell=True,
    #     debug=True
    # )
    # p.join()

    # copy specific rows in correct table order
    source = MySQL(config.source)

    destination = MySQL(schema="information_schema", kwargs=config.destination)
    with destination.transaction() as t:
        destination.execute("drop database if exists " + quote_column(config.destination.schema))
        destination.execute("create database " + quote_column(config.destination.schema))

    # fill new db using extract
    p = Process(
        "make new db",
        [
            config.mysql,
            "-u", config.destination.username,
            "-p" + config.destination.password,
            "-P", coalesce(config.destination.port, 3306),
            "-h", config.destination.host,
            config.destination.schema,
            "<", config.schema_file
        ],
        shell=True,
        debug=True
    )
    p.join()

    destination = MySQL(config.destination)

    def copy(query):
        try:
            table_name = query['from']
            filter = esfilter2sqlwhere(coalesce(query.where, True))
            Log.note("copy {{table}}", table=table_name)

            records = source.query(
                SQL_SELECT + " * " +
                SQL_FROM + quote_column(table_name) +
                SQL_WHERE + filter
            )

            destination.insert_list(
                table_name,
                records
            )
        except Exception as e:
            Log.warning("Can not copy table {{table}}", table=table_name, cause=e)

    with destination.transaction():
        destination.execute("ALTER TABLE tag DROP FOREIGN KEY fk_tag_user_id_profiles_userid")
        destination.execute("ALTER TABLE dependencies DROP FOREIGN KEY fk_dependencies_blocked_bugs_bug_id")
        destination.execute("ALTER TABLE dependencies DROP FOREIGN KEY fk_dependencies_dependson_bugs_bug_id")
        destination.execute("ALTER TABLE duplicates DROP FOREIGN KEY fk_duplicates_dupe_bugs_bug_id")
        destination.execute("ALTER TABLE duplicates DROP FOREIGN KEY fk_duplicates_dupe_of_bugs_bug_id")

        for query in config.full_copy:
            copy(query)

        products = source.query(
            "SELECT id, name, description, classification_id FROM products WHERE id IN (SELECT product_id FROM bugs WHERE bug_id IN " + quote_list(config.bug_list) + ")"
        )
        destination.insert_list("products", products)

        components = source.query(
            "SELECT * FROM components WHERE id IN (SELECT component_id FROM bugs WHERE bug_id IN " + quote_list(config.bug_list) + ")"
        )

        profiles = source.query(
            SQL("SELECT `comment_count`, `creation_ts`, `disable_mail`, `disabledtext`, `extern_id`, `feedback_request_count`, `first_patch_bug_id`, `first_patch_reviewed_id`, `is_enabled`, `last_activity_ts`, `last_seen_date`, `last_statistics_ts`, `login_name`, `mfa`, `mfa_required_date`, `mybugslink`, `needinfo_request_count`, `password_change_reason`, `password_change_required`, `realname`, `review_request_count`, `userid`")+
            "FROM profiles WHERE userid IN (" +
            "SELECT reporter as id FROM bugs WHERE bug_id IN " + quote_list(config.bug_list) +
            SQL_UNION +
            "SELECT assigned_to as id FROM bugs WHERE bug_id IN " + quote_list(config.bug_list) +
            SQL_UNION +
            "SELECT qa_contact as id FROM bugs WHERE bug_id IN " + quote_list(config.bug_list) +
            SQL_UNION +
            "SELECT who as id FROM cc WHERE bug_id IN " + quote_list(config.bug_list) +
            SQL_UNION +
            "SELECT submitter_id as id FROM attachments WHERE bug_id IN " + quote_list(config.bug_list) +
            SQL_UNION +
            "SELECT who as id FROM bugs_activity WHERE bug_id IN " + quote_list(config.bug_list) +
            SQL_UNION +
            "SELECT setter_id as id FROM flags WHERE bug_id IN " + quote_list(config.bug_list) +
            SQL_UNION +
            "SELECT requestee_id as id FROM flags WHERE bug_id IN " + quote_list(config.bug_list) +
            SQL_UNION +
            "SELECT who as id FROM longdescs WHERE bug_id IN " + quote_list(config.bug_list) +
            SQL_UNION +
            "SELECT initialowner as id FROM components WHERE id IN " + quote_list(components.id) +
            SQL_UNION +
            "SELECT initialqacontact as id FROM components WHERE id IN " + quote_list(components.id) +
            SQL_UNION +
            "SELECT watch_user as id FROM components WHERE id IN " + quote_list(components.id) +
            SQL_UNION +
            "SELECT triage_owner_id as id FROM components WHERE id IN " + quote_list(components.id) +
            ")"
        )
        destination.insert_list("profiles", profiles)
        destination.insert_list("components", components)

        flagtypes = source.query(
            "SELECT id, name, 'none' as description FROM flagtypes"
        )
        destination.insert_list("flagtypes", flagtypes)

        milestones = source.query(
            "SELECT * FROM milestones WHERE product_id IN " + quote_list(products.id)
        )
        destination.insert_list("milestones", milestones)

        versions = source.query(
            "SELECT * FROM versions WHERE product_id IN " + quote_list(products.id)
        )
        destination.insert_list("versions", versions)

        for query in config['copy']:
            copy(query)


        longdescs = source.query(
            "SELECT * FROM longdescs WHERE bug_id IN " + quote_list(config.bug_list)
        )
        for l in longdescs:
            destination.insert("longdescs", l)

    # use mysqldump, again to get full extract as file
    # "C:\Program Files\MySQL\MySQL Server 5.5\bin\mysqldump.exe" --skip-tz-utc -u root -p{{password}} -h klahnakoski-es.corp.tor1.mozilla.com bugzilla > small_bugzilla.sql
    p=Process(
        "extract mini Bugzilla",
        [
            config.mysqldump,
            "-u", config.destination.username,
            "-p" + config.destination.password,
            "-P", coalesce(config.destination.port, 3306),
            "-h", config.destination.host,
            config.destination.schema,
            ">", config.mini_file
        ],
        shell=True,
        debug=True
    )
    p.join()





def setup():
    try:
        settings = startup.read_settings()
        constants.set(settings.constants)
        Log.start(settings.debug)
        bugzilla_extract(settings)
    except Exception as e:
        Log.fatal("Can not start", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    setup()
