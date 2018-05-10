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

from pyLibrary.sql.sqlite import quote_column

from jx_mysql import esfilter2sqlwhere
from mo_dots import listwrap
from mo_files import File
from mo_logs import startup, constants, Log
from mo_threads import Process
from pyLibrary.sql import SQL_WHERE, SQL_SELECT, SQL_FROM
from pyLibrary.sql.mysql import MySQL


def bugzilla_extract(config):


    # extract schema with mysqldump
    # mysqldump -u root -p --no-data dbname > schema.sql
    Process(
        "extract Bugzilla schema only",
        [
            config.mysqldump,
            "-u", config.source.username,
            "-p" + config.source.password,
            "--no-data", config.source.schema,
            ">", config.schema_file
        ],
        shell=True
    )

    # make new db using extract
    Process(
        "make new db",
        [
            config.mysql,
            "-u", config.destination.username,
            "-p" + config.destination.password,
            config.destination.schema,
            "<", config.schema_file
        ],
        shell=True
    )

    # copy specific rows in correct table order
    source = MySQL(config.source)
    destination = MySQL(config.destination)

    def copy(query):
        table_name= query['from']
        filter = esfilter2sqlwhere(query.where)

        records = source.query(
            SQL_SELECT + " * " +
            SQL_FROM + quote_column(table_name) +
            SQL_WHERE + filter
        )

        destination.insert_list(
            table_name,
            records
        )

    for query in config.copy:
        copy(query)

    # use mysqldump, again to get full extract as file
    # "C:\Program Files\MySQL\MySQL Server 5.5\bin\mysqldump.exe" --skip-tz-utc -u root -p{{password}} -h klahnakoski-es.corp.tor1.mozilla.com bugzilla > small_bugzilla.sql
    Process(
        "extract mini Bugzilla",
        [
            config.mysqldump,
            "-u", config.destination.username,
            "-p" + config.destination.password,
            "--no-data", config.destination.schema,
            ">", config.mini_file
        ],
        shell=True
    )




def setup():
    try:
        settings = startup.read_settings()
        constants.set(settings.constants)

        with startup.SingleInstance(flavor_id=settings.args.filename):
            if settings.args.restart:
                for l in listwrap(settings.debug.log):
                    if l.filename:
                        File(l.filename).delete()
                File(settings.param.first_run_time).delete()
                File(settings.param.last_run_time).delete()

            Log.start(settings.debug)
            bugzilla_extract(settings)
    except Exception as e:
        Log.fatal("Can not start", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    setup()
