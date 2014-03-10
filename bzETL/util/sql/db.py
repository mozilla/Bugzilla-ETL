# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals

from datetime import datetime
import json
import subprocess
from pymysql import connect, InterfaceError
from .. import struct
from ..jsons import json_scrub
from ..maths import Math
from ..strings import expand_template
from ..struct import nvl, wrap
from ..cnv import CNV
from ..env.logs import Log, Except
from ..queries import Q
from ..strings import indent
from ..strings import outdent
from ..env.files import File


DEBUG = False
MAX_BATCH_SIZE = 100

all_db = []


class DB(object):
    """
    Parameterize SQL by name rather than by position.  Return records as objects
    rather than tuples.
    """

    def __init__(self, settings, schema=None, preamble=None, readonly=False):
        """
        OVERRIDE THE settings.schema WITH THE schema PARAMETER
        preamble WILL BE USED TO ADD COMMENTS TO THE BEGINNING OF ALL SQL
        THE INTENT IS TO HELP ADMINISTRATORS ID THE SQL RUNNING ON THE DATABASE

        schema - NAME OF DEFAULT database/schema IN QUERIES

        preamble - A COMMENT TO BE ADDED TO EVERY SQL STATEMENT SENT

        readonly - USED ONLY TO INDICATE IF A TRANSACTION WILL BE OPENED UPON
        USE IN with CLAUSE, YOU CAN STILL SEND UPDATES, BUT MUST OPEN A
        TRANSACTION BEFORE YOU DO
        """
        if settings == None:
            Log.warning("No settings provided")
            return

        all_db.append(self)

        if isinstance(settings, DB):
            settings = settings.settings

        self.settings = settings.copy()
        self.settings.schema = nvl(schema, self.settings.schema, self.settings.database)

        preamble = nvl(preamble, self.settings.preamble)
        if preamble == None:
            self.preamble = ""
        else:
            self.preamble = indent(preamble, "# ").strip() + "\n"

        self.readonly = readonly
        self.debug = nvl(self.settings.debug, DEBUG)
        self._open()

    def _open(self):
        """ DO NOT USE THIS UNLESS YOU close() FIRST"""
        try:
            self.db = connect(
                host=self.settings.host,
                port=self.settings.port,
                user=nvl(self.settings.username, self.settings.user),
                passwd=nvl(self.settings.password, self.settings.passwd),
                db=nvl(self.settings.schema, self.settings.db),
                charset=u"utf8",
                use_unicode=True
            )
        except Exception, e:
            if self.settings.host.find("://") == -1:
                Log.error(u"Failure to connect", e)
            else:
                Log.error(u"Failure to connect.  PROTOCOL PREFIX IS PROBABLY BAD", e)
        self.cursor = None
        self.partial_rollback = False
        self.transaction_level = 0
        self.backlog = []     #accumulate the write commands so they are sent at once


    def __enter__(self):
        if not self.readonly:
            self.begin()
        return self

    def __exit__(self, type, value, traceback):
        if self.readonly:
            self.close()
            return

        if isinstance(value, BaseException):
            try:
                if self.cursor: self.cursor.close()
                self.cursor = None
                self.rollback()
            except Exception, e:
                Log.warning(u"can not rollback()", [value, e])
            finally:
                self.close()
            return

        try:
            self.commit()
        except Exception, e:
            Log.warning(u"can not commit()", e)
        finally:
            self.close()


    def transaction(self):
        """
        return not-started transaction (for with statement)
        """
        return Transaction(self)

    def begin(self):
        if self.transaction_level == 0:
            self.cursor = self.db.cursor()
        self.transaction_level += 1
        self.execute("SET TIME_ZONE='+00:00'")


    def close(self):
        if self.transaction_level > 0:
            Log.error("expecting commit() or rollback() before close")
        self.cursor = None  #NOT NEEDED
        try:
            self.db.close()
        except Exception, e:
            if e.message.find("Already closed") >= 0:
                return

            Log.warning("can not close()", e)
        finally:
            all_db.remove(self)

    def commit(self):
        try:
            self._execute_backlog()
        except Exception, e:
            try:
                self.rollback()
            except Exception:
                pass
            Log.error("Error while processing backlog", e)

        if self.transaction_level == 0:
            Log.error("No transaction has begun")
        elif self.transaction_level == 1:
            if self.partial_rollback:
                try:
                    self.rollback()
                except Exception:
                    pass
                Log.error("Commit after nested rollback is not allowed")
            else:
                if self.cursor: self.cursor.close()
                self.cursor = None
                self.db.commit()

        self.transaction_level -= 1

    def flush(self):
        try:
            self.commit()
        except Exception, e:
            Log.error("Can not flush", e)

        try:
            self.begin()
        except Exception, e:
            Log.error("Can not flush", e)


    def rollback(self):
        self.backlog = []     #YAY! FREE!
        if self.transaction_level == 0:
            Log.error("No transaction has begun")
        elif self.transaction_level == 1:
            self.transaction_level -= 1
            if self.cursor != None:
                self.cursor.close()
            self.cursor = None
            self.db.rollback()
        else:
            self.transaction_level -= 1
            self.partial_rollback = True
            Log.warning("Can not perform partial rollback!")


    def call(self, proc_name, params):
        self._execute_backlog()
        params = [struct.unwrap(v) for v in params]
        try:
            self.cursor.callproc(proc_name, params)
            self.cursor.close()
            self.cursor = self.db.cursor()
        except Exception, e:
            Log.error("Problem calling procedure " + proc_name, e)


    def query(self, sql, param=None):
        """
        RETURN RESULTS IN [row_num][column] GRID
        """
        self._execute_backlog()
        try:
            old_cursor = self.cursor
            if not old_cursor: #ALLOW NON-TRANSACTIONAL READS
                self.cursor = self.db.cursor()
                self.cursor.execute("SET TIME_ZONE='+00:00'")
                self.cursor.close()
                self.cursor = self.db.cursor()

            if param:
                sql = expand_template(sql, self.quote_param(param))
            sql = self.preamble + outdent(sql)
            if self.debug:
                Log.note("Execute SQL:\n{{sql}}", {"sql": indent(sql)})

            self.cursor.execute(sql)
            columns = [utf8_to_unicode(d[0]) for d in nvl(self.cursor.description, [])]
            fixed = [[utf8_to_unicode(c) for c in row] for row in self.cursor]
            result = CNV.table2list(columns, fixed)

            if not old_cursor:   #CLEANUP AFTER NON-TRANSACTIONAL READS
                self.cursor.close()
                self.cursor = None

            return result
        except Exception, e:
            if isinstance(e, InterfaceError) or e.message.find("InterfaceError") >= 0:
                Log.error("Did you close the db connection?", e)
            Log.error("Problem executing SQL:\n" + indent(sql.strip()), e, offset=1)

    def column_query(self, sql, param=None):
        """
        RETURN RESULTS IN [column][row_num] GRID
        """
        self._execute_backlog()
        try:
            old_cursor = self.cursor
            if not old_cursor: #ALLOW NON-TRANSACTIONAL READS
                self.cursor = self.db.cursor()
                self.cursor.execute("SET TIME_ZONE='+00:00'")
                self.cursor.close()
                self.cursor = self.db.cursor()

            if param:
                sql = expand_template(sql, self.quote_param(param))
            sql = self.preamble + outdent(sql)
            if self.debug:
                Log.note("Execute SQL:\n{{sql}}", {"sql": indent(sql)})

            self.cursor.execute(sql)
            grid = [[utf8_to_unicode(c) for c in row] for row in self.cursor]
            # columns = [utf8_to_unicode(d[0]) for d in nvl(self.cursor.description, [])]
            result = zip(*grid)

            if not old_cursor:   #CLEANUP AFTER NON-TRANSACTIONAL READS
                self.cursor.close()
                self.cursor = None

            return result
        except Exception, e:
            if isinstance(e, InterfaceError) or e.message.find("InterfaceError") >= 0:
                Log.error("Did you close the db connection?", e)
            Log.error("Problem executing SQL:\n" + indent(sql.strip()), e, offset=1)




    # EXECUTE GIVEN METHOD FOR ALL ROWS RETURNED
    def forall(self, sql, param=None, _execute=None):
        assert _execute
        num = 0

        self._execute_backlog()
        try:
            old_cursor = self.cursor
            if not old_cursor: #ALLOW NON-TRANSACTIONAL READS
                self.cursor = self.db.cursor()

            if param:
                sql = expand_template(sql, self.quote_param(param))
            sql = self.preamble + outdent(sql)
            if self.debug:
                Log.note("Execute SQL:\n{{sql}}", {"sql": indent(sql)})
            self.cursor.execute(sql)

            columns = tuple([utf8_to_unicode(d[0]) for d in self.cursor.description])
            for r in self.cursor:
                num += 1
                _execute(wrap(dict(zip(columns, [utf8_to_unicode(c) for c in r]))))

            if not old_cursor:   #CLEANUP AFTER NON-TRANSACTIONAL READS
                self.cursor.close()
                self.cursor = None

        except Exception, e:
            Log.error("Problem executing SQL:\n" + indent(sql.strip()), e, offset=1)

        return num


    def execute(self, sql, param=None):
        if self.transaction_level == 0:
            Log.error("Expecting transaction to be started before issuing queries")

        if param:
            sql = expand_template(sql, self.quote_param(param))
        sql = outdent(sql)
        self.backlog.append(sql)
        if self.debug or len(self.backlog) >= MAX_BATCH_SIZE:
            self._execute_backlog()


    def execute_file(self, filename, param=None):
        content = File(filename).read()
        self.execute(content, param)

    @staticmethod
    def execute_sql(settings, sql, param=None):
        """EXECUTE MANY LINES OF SQL (FROM SQLDUMP FILE, MAYBE?"""

        if param:
            with DB(settings) as temp:
                sql = expand_template(sql, temp.quote_param(param))

        # MWe have no way to execute an entire SQL file in bulk, so we
        # have to shell out to the commandline client.
        args = [
            "mysql",
            "-h{0}".format(settings.host),
            "-u{0}".format(settings.username),
            "-p{0}".format(settings.password),
            "{0}".format(settings.schema)
        ]

        proc = subprocess.Popen(
            args,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=-1
        )
        if isinstance(sql, unicode):
            sql = sql.encode("utf8")
        (output, _) = proc.communicate(sql)

        if proc.returncode:
            if len(sql) > 10000:
                sql = "<" + unicode(len(sql)) + " bytes of sql>"
            Log.error("Unable to execute sql: return code {{return_code}}, {{output}}:\n {{sql}}\n", {
                "sql": indent(sql),
                "return_code": proc.returncode,
                "output": output
            })

    @staticmethod
    def execute_file(settings, filename, param=None):
        # MySQLdb provides no way to execute an entire SQL file in bulk, so we
        # have to shell out to the commandline client.
        sql = File(filename).read()
        DB.execute_sql(settings, sql, param)


    def _execute_backlog(self):
        if not self.backlog: return

        (backlog, self.backlog) = (self.backlog, [])
        if self.db.__module__.startswith("pymysql"):
            # BUG IN PYMYSQL: CAN NOT HANDLE MULTIPLE STATEMENTS
            # https://github.com/PyMySQL/PyMySQL/issues/157
            for b in backlog:
                sql = self.preamble + b
                try:
                    if self.debug:
                        Log.note("Execute SQL:\n{{sql|indent}}", {"sql": sql})
                    self.cursor.execute(b)
                except Exception, e:
                    Log.error("Can not execute sql:\n{{sql}}", {"sql": sql}, e)

            self.cursor.close()
            self.cursor = self.db.cursor()
        else:
            for i, g in Q.groupby(backlog, size=MAX_BATCH_SIZE):
                sql = self.preamble + ";\n".join(g)
                try:
                    if self.debug:
                        Log.note("Execute block of SQL:\n{{sql|indent}}", {"sql": sql})
                    self.cursor.execute(sql)
                    self.cursor.close()
                    self.cursor = self.db.cursor()
                except Exception, e:
                    Log.error("Problem executing SQL:\n{{sql}}", {"sql": indent(sql.strip())}, e, offset=1)


    ## Insert dictionary of values into table
    def insert(self, table_name, record):
        keys = record.keys()

        try:
            command = "INSERT INTO " + self.quote_column(table_name) + "(" + \
                      ",".join([self.quote_column(k) for k in keys]) + \
                      ") VALUES (" + \
                      ",".join([self.quote_value(record[k]) for k in keys]) + \
                      ")"

            self.execute(command)
        except Exception, e:
            Log.error("problem with record: {{record}}", {"record": record}, e)

    # candidate_key IS LIST OF COLUMNS THAT CAN BE USED AS UID (USUALLY PRIMARY KEY)
    # ONLY INSERT IF THE candidate_key DOES NOT EXIST YET
    def insert_new(self, table_name, candidate_key, new_record):
        candidate_key = struct.listwrap(candidate_key)

        condition = " AND\n".join([self.quote_column(k) + "=" + self.quote_value(new_record[k]) if new_record[k] != None else self.quote_column(k) + " IS Null" for k in candidate_key])
        command = "INSERT INTO " + self.quote_column(table_name) + " (" + \
                  ",".join([self.quote_column(k) for k in new_record.keys()]) + \
                  ")\n" + \
                  "SELECT a.* FROM (SELECT " + ",".join([self.quote_value(v) + " " + self.quote_column(k) for k, v in new_record.items()]) + " FROM DUAL) a\n" + \
                  "LEFT JOIN " + \
                  "(SELECT 'dummy' exist FROM " + self.quote_column(table_name) + " WHERE " + condition + " LIMIT 1) b ON 1=1 WHERE exist IS Null"
        self.execute(command, {})


    # ONLY INSERT IF THE candidate_key DOES NOT EXIST YET
    def insert_newlist(self, table_name, candidate_key, new_records):
        for r in new_records:
            self.insert_new(table_name, candidate_key, r)


    def insert_list(self, table_name, records):
        if not records:
            return

        keys = set()
        for r in records:
            keys |= set(r.keys())
        keys = Q.sort(keys)

        try:
            command = \
                "INSERT INTO " + self.quote_column(table_name) + "(" + \
                ",".join([self.quote_column(k) for k in keys]) + \
                ") VALUES " + ",\n".join([
                    "(" + ",".join([self.quote_value(r[k]) for k in keys]) + ")"
                    for r in records
                ])
            self.execute(command)
        except Exception, e:
            Log.error("problem with record: {{record}}", {"record": records}, e)


    def update(self, table_name, where_slice, new_values):
        """
        where_slice IS A Struct WHICH WILL BE USED TO MATCH ALL IN table
        """
        new_values = self.quote_param(new_values)

        where_clause = " AND\n".join([
            self.quote_column(k) + "=" + self.quote_value(v) if v != None else self.quote_column(k) + " IS NULL"
            for k, v in where_slice.items()]
        )

        command = "UPDATE " + self.quote_column(table_name) + "\n" + \
                  "SET " + \
                  ",\n".join([self.quote_column(k) + "=" + v for k, v in new_values.items()]) + "\n" + \
                  "WHERE " + \
                  where_clause
        self.execute(command, {})


    def quote_param(self, param):
        return {k: self.quote_value(v) for k, v in param.items()}

    def quote_value(self, value):
        """
        convert values to mysql code for the same
        mostly delegate directly to the mysql lib, but some exceptions exist
        """
        try:
            if value == None:
                return "NULL"
            elif isinstance(value, SQL):
                if not value.param:
                    #value.template CAN BE MORE THAN A TEMPLATE STRING
                    return self.quote_sql(value.template)
                param = {k: self.quote_sql(v) for k, v in value.param.items()}
                return expand_template(value.template, param)
            elif isinstance(value, basestring):
                return self.db.literal(value)
            elif isinstance(value, datetime):
                return "str_to_date('" + value.strftime("%Y%m%d%H%M%S") + "', '%Y%m%d%H%i%s')"
            elif hasattr(value, '__iter__'):
                return self.db.literal(json_encode(value))
            elif isinstance(value, dict):
                return self.db.literal(json_encode(value))
            elif Math.is_number(value):
                return unicode(value)
            else:
                return self.db.literal(value)
        except Exception, e:
            Log.error("problem quoting SQL", e)


    def quote_sql(self, value, param=None):
        """
        USED TO EXPAND THE PARAMETERS TO THE SQL() OBJECT
        """
        try:
            if isinstance(value, SQL):
                if not param:
                    return value
                param = {k: self.quote_sql(v) for k, v in param.items()}
                return expand_template(value, param)
            elif isinstance(value, basestring):
                return value
            elif isinstance(value, dict):
                return self.db.literal(json_encode(value))
            elif hasattr(value, '__iter__'):
                return "(" + ",".join([self.quote_sql(vv) for vv in value]) + ")"
            else:
                return unicode(value)
        except Exception, e:
            Log.error("problem quoting SQL", e)

    def quote_column(self, column_name, table=None):
        if isinstance(column_name, basestring):
            if table:
                column_name = table + "." + column_name
            return SQL("`" + column_name.replace(".", "`.`") + "`")    #MY SQL QUOTE OF COLUMN NAMES
        elif isinstance(column_name, list):
            if table:
                return SQL(", ".join([self.quote_column(table + "." + c) for c in column_name]))
            return SQL(", ".join([self.quote_column(c) for c in column_name]))
        else:
            #ASSUME {"name":name, "value":value} FORM
            return SQL(column_name.value + " AS " + self.quote_column(column_name.name))

    def sort2sqlorderby(self, sort):
        sort = Q.normalize_sort_parameters(sort)
        return ",\n".join([self.quote_column(s.field) + (" DESC" if s.sort == -1 else " ASC") for s in sort])




def utf8_to_unicode(v):
    try:
        if isinstance(v, str):
            return v.decode("utf8")
        else:
            return v
    except Exception, e:
        Log.error("not expected", e)


#ACTUAL SQL, DO NOT QUOTE THIS STRING
class SQL(unicode):
    def __init__(self, template='', param=None):
        unicode.__init__(self)
        self.template = template
        self.param = param

    @property
    def sql(self):
        return expand_template(self.template, self.param)

    def __str__(self):
        Log.error("do not do this")


def int_list_packer(term, values):
    """
    return singletons, ranges and exclusions
    """
    DENSITY = 10  #a range can have holes, this is inverse of the hole density
    MIN_RANGE = 20  #min members before a range is allowed to be used

    singletons = set()
    ranges = []
    exclude = set()

    sorted = Q.sort(values)

    last = sorted[0]
    curr_start = last
    curr_excl = set()

    for v in sorted[1::]:
        if v <= last + 1:
            pass
        elif v - last > 3:
            #big step, how do we deal with it?
            if last == curr_start:
                #not a range yet, so just add as singlton
                singletons.add(last)
            elif last - curr_start - len(curr_excl) < MIN_RANGE or ((last - curr_start) < len(curr_excl) * DENSITY):
                #small ranges are singletons, sparse ranges are singletons
                singletons |= set(range(curr_start, last + 1))
                singletons -= curr_excl
            else:
                #big enough, and dense enough range
                ranges.append({"gte": curr_start, "lte": last})
                exclude |= curr_excl
            curr_start = v
            curr_excl = set()
        else:
            if 1 + last - curr_start >= len(curr_excl) * DENSITY:
                #high density, keep track of excluded and continue
                add_me = set(range(last + 1, v))
                curr_excl |= add_me
            elif 1 + last - curr_start - len(curr_excl) < MIN_RANGE:
                #not big enough, convert range to singletons
                new_singles = set(range(curr_start, last + 1)) - curr_excl
                singletons = singletons | new_singles

                curr_start = v
                curr_excl = set()
            else:
                ranges.append({"gte": curr_start, "lte": last})
                exclude |= curr_excl
                curr_start = v
                curr_excl = set()
        last = v

    if last == curr_start:
        #not a range yet, so just add as singlton
        singletons.add(last)
    elif last - curr_start - len(curr_excl) < MIN_RANGE or ((last - curr_start) < len(curr_excl) * DENSITY):
        #small ranges are singletons, sparse ranges are singletons
        singletons |= set(range(curr_start, last + 1))
        singletons -= curr_excl
    else:
        #big enough, and dense enough range
        ranges.append({"gte": curr_start, "lte": last})
        exclude |= curr_excl

    if ranges:
        r = {"or": [{"range": {term: r}} for r in ranges]}
        if exclude:
            r = {"and": [r, {"not": {"terms": {term: Q.sort(exclude)}}}]}
        if singletons:
            return {"or": [
                {"terms": {term: Q.sort(singletons)}},
                r
            ]}
        else:
            return r
    else:
        raise Except("no packing possible")


class Transaction(object):
    def __init__(self, db):
        self.db = db

    def __enter__(self):
        self.db.begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if isinstance(exc_val, Exception):
            self.db.rollback()
        else:
            self.db.commit()


# FOR PUTTING JSON INTO DATABASE (sort_keys=True)
# dicts CAN BE USED AS KEYS
def json_encode(value):
    return unicode(json_encoder.encode(json_scrub(value)))

json_encoder = json.JSONEncoder(
    skipkeys=False,
    ensure_ascii=False,  # DIFF FROM DEFAULTS
    check_circular=True,
    allow_nan=True,
    indent=None,
    separators=None,
    encoding='utf-8',
    default=None,
    sort_keys=True
)

