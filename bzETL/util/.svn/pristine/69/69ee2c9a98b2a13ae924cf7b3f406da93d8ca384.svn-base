################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################


from datetime import datetime
from decimal import Decimal
from pymysql import connect
from . import struct
from .maths import Math
from .strings import expand_template
from .basic import nvl
from .cnv import CNV
from .debug import D
from .struct import Struct
from .query import Q
from .strings import indent
from .strings import outdent
from .files import File



DEBUG = False
MAX_BATCH_SIZE=100

class DB():
    """

    """

    def __init__(self, settings, schema=None):
        """OVERRIDE THE settings.schema WITH THE schema PARAMETER"""
        if isinstance(settings, DB):
            settings=settings.settings

        self.settings=settings.copy()
        self.settings.schema=nvl(schema, self.settings.schema)

        self.debug=nvl(self.settings.debug, DEBUG)
        self._open()

    def _open(self):
        """ DO NOT USE THIS UNLESS YOU close() FIRST"""
        try:
            self.db=connect(
                host=self.settings.host,
                port=self.settings.port,
                user=nvl(self.settings.username, self.settings.user),
                passwd=nvl(self.settings.password, self.settings.passwd),
                db=nvl(self.settings.schema, self.settings.schema, self.settings.db),
                charset="utf8",
                use_unicode=True
            )
        except Exception, e:
            D.error("Failure to connect", e)
        self.cursor=None
        self.partial_rollback=False
        self.transaction_level=0
        self.backlog=[]     #accumulate the write commands so they are sent at once


    def __enter__(self):
        self.begin()
        return self

    def __exit__(self, type, value, traceback):
        if isinstance(value, BaseException):
            try:
                if self.cursor: self.cursor.close()
                self.cursor=None
                self.rollback()
            except Exception, e:
                D.warning("can not rollback()", e)
            finally:
                self.close()
            return

        try:
            self.commit()
        except Exception, e:
            D.warning("can not commit()", e)
        finally:
            self.close()


    def begin(self):
        if self.transaction_level==0: self.cursor=self.db.cursor()
        self.transaction_level+=1


    def close(self):
        if self.transaction_level>0:
            D.error("expecting commit() or rollback() before close")
        self.cursor=None  #NOT NEEDED
        try:
            self.db.close()
        except Exception, e:
            D.warning("can not close()", e)

    def commit(self):
        try:
            self._execute_backlog()
        except Exception, e:
            try:
                self.rollback()
            except Exception:
                pass
            D.error("Error while processing backlog", e)
            
        if self.transaction_level==0:
            D.error("No transaction has begun")
        elif self.transaction_level==1:
            if self.partial_rollback:
                try:
                    self.rollback()
                except Exception:
                    pass
                D.error("Commit after nested rollback is not allowed")
            else:
                if self.cursor: self.cursor.close()
                self.cursor=None
                self.db.commit()

        self.transaction_level-=1

    def flush(self):
        try:
            self.commit()
        except Exception, e:
            D.error("Can not flush", e)

        try:
            self.begin()
        except Exception, e:
            D.error("Can not flush", e)


    def rollback(self):
        self.backlog=[]     #YAY! FREE!
        if self.transaction_level==0:
            D.error("No transaction has begun")
        elif self.transaction_level==1:
            self.transaction_level-=1
            if self.cursor: self.cursor.close()
            self.cursor=None
            self.db.rollback()
        else:
            self.transaction_level-=1
            self.partial_rollback=True
            D.warning("Can not perform partial rollback!")



    def call(self, proc_name, params):
        self._execute_backlog()
        try:
            self.cursor.callproc(proc_name, params)
            self.cursor.close()
            self.cursor=self.db.cursor()
        except Exception, e:
            D.error("Problem calling procedure "+proc_name, e)



    def query(self, sql, param=None):
        self._execute_backlog()
        try:
            old_cursor=self.cursor
            if old_cursor is None: #ALLOW NON-TRANSACTIONAL READS
                self.cursor=self.db.cursor()

            if param is not None: sql=expand_template(sql, self.quote_param(param))
            sql=outdent(sql)
            if self.debug: D.println("Execute SQL:\n{{sql}}", {"sql":indent(sql)})

            self.cursor.execute(sql)

            columns = [utf8_to_unicode(d[0]) for d in nvl(self.cursor.description, [])]
            fixed=[[utf8_to_unicode(c) for c in row] for row in self.cursor]
            result=CNV.table2list(columns, fixed)

            if old_cursor is None:   #CLEANUP AFTER NON-TRANSACTIONAL READS
                self.cursor.close()
                self.cursor=None

            return result
        except Exception, e:
            D.error("Problem executing SQL:\n"+indent(sql.strip()), e, offset=1)

            
    # EXECUTE GIVEN METHOD FOR ALL ROWS RETURNED
    def foreach(self, sql, param=None, execute=None):
        assert execute is not None

        num=0

        self._execute_backlog()
        try:
            old_cursor=self.cursor
            if old_cursor is None: #ALLOW NON-TRANSACTIONAL READS
                self.cursor=self.db.cursor()

            if param is not None: sql=expand_template(sql,self.quote_param(param))
            sql=outdent(sql)
            if self.debug: D.println("Execute SQL:\n{{sql}}", {"sql":indent(sql)})

            self.cursor.execute(sql)

            columns = tuple( [utf8_to_unicode(d[0]) for d in self.cursor.description] )
            for r in self.cursor:
                num+=1
                execute(struct.wrap(dict(zip(columns, [utf8_to_unicode(c) for c in r]))))

            if old_cursor is None:   #CLEANUP AFTER NON-TRANSACTIONAL READS
                self.cursor.close()
                self.cursor=None

        except Exception, e:
            D.error("Problem executing SQL:\n"+indent(sql.strip()), e, offset=1)

        return num

    
    def execute(self, sql, param=None):
        if self.transaction_level==0: D.error("Expecting transation to be started before issuing queries")

        if param is not None: sql=expand_template(sql,self.quote_param(param))
        sql=outdent(sql)
        self.backlog.append(sql)
        if len(self.backlog)>=MAX_BATCH_SIZE:
            self._execute_backlog()

        
    def execute_file(self, filename, param=None):
        content=File(filename).read()
        self.execute(content, param)

    @staticmethod
    def execute_sql(settings, sql, param=None):
        """EXECUTE MANY LINES OF SQL (FROM SQLDUMP FILE, MAYBE?"""

        # MySQLdb provides no way to execute an entire SQL file in bulk, so we
        # have to shell out to the commandline client.
        if param is not None: sql=expand_template(sql,param)
        
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
            )
        (output, _) = proc.communicate(sql)

        if proc.returncode:
            D.error("Unable to execute sql: return code {{return_code}}, {{output}}:\n {{sql}}\n",
                    {"sql":indent(sql), "return_code":proc.returncode, "output":output})

    @staticmethod
    def execute_file(settings, filename, param=None):
        # MySQLdb provides no way to execute an entire SQL file in bulk, so we
        # have to shell out to the commandline client.
        sql=File(filename).read()
        DB.execute_sql(settings, sql, param)


    def _execute_backlog(self):
        if len(self.backlog)==0: return

        (backlog, self.backlog)=(self.backlog, [])
        if self.db.__module__.startswith("pymysql"):
            #BUG IN PYMYSQL: CAN NOT HANDLE MULTIPLE STATEMENTS
            for b in backlog:
                try:
                    self.cursor.execute(b)
                except Exception, e:
                    D.error("Can not execute sql:\n{{sql}}", {"sql":b}, e)
            self.cursor.close()
            self.cursor = self.db.cursor()
        else:
            for i, g in Q.groupby(backlog, size=MAX_BATCH_SIZE):
                sql=";\n".join(g)
                try:
                    if self.debug: D.println("Execute block of SQL:\n"+indent(sql))
                    self.cursor.execute(sql)
                    self.cursor.close()
                    self.cursor = self.db.cursor()
                except Exception, e:
                    D.error("Problem executing SQL:\n{{sql}}", {"sql":indent(sql.strip())}, e, offset=1)




    ## Insert dictionary of values into table
    def insert (self, table_name, record):
        keys = record.keys()

        try:
            command = "INSERT INTO "+self.quote_column(table_name)+"("+\
                      ",".join([self.quote_column(k) for k in keys])+\
                      ") VALUES ("+\
                      ",".join([self.quote_value(record[k]) for k in keys])+\
                      ")"

            self.execute(command)
        except Exception, e:
            D.error("problem with record: {{record}}", {"record":record}, e)

    # candidate_key IS LIST OF COLUMNS THAT CAN BE USED AS UID (USUALLY PRIMARY KEY)
    # ONLY INSERT IF THE candidate_key DOES NOT EXIST YET
    def insert_new(self, table_name, candidate_key, new_record):
        if not isinstance(candidate_key, list): candidate_key=[candidate_key]

        condition=" AND\n".join([self.quote_column(k)+"="+self.quote_value(new_record[k]) if new_record[k] is not None else self.quote_column(k)+" IS NULL"  for k in candidate_key])
        command="INSERT INTO "+self.quote_column(table_name)+" ("+\
                ",".join([self.quote_column(k) for k in new_record.keys()])+\
                ")\n"+\
                "SELECT a.* FROM (SELECT "+",".join([self.quote_value(v)+" "+self.quote_column(k) for k,v in new_record.items()])+" FROM DUAL) a\n"+\
                "LEFT JOIN "+\
                "(SELECT 'dummy' exist FROM "+self.quote_column(table_name)+" WHERE "+condition+" LIMIT 1) b ON 1=1 WHERE exist IS NULL"
        self.execute(command, {})


    # ONLY INSERT IF THE candidate_key DOES NOT EXIST YET
    def insert_newlist(self, table_name, candidate_key, new_records):
        for r in new_records:
            self.insert_new(table_name, candidate_key, r)


    def insert_list(self, table_name, records):
        #PROBABLY CAN BE BETTER DONE WITH executeMany()
        for r in records:
            self.insert(table_name, r)


    def update(self, table_name, where, new_values):

        where=self.quote_param(where)
        new_values=self.quote_param(new_values)

        command="UPDATE "+self.quote_column(table_name)+"\n"+\
                "SET "+\
                ",\n".join([self.quote_column(k)+"="+v for k,v in new_values.items()])+"\n"+\
                "WHERE "+\
                " AND\n".join([self.quote_column(k)+"="+v for k,v in where.items()])
        self.execute(command, {})


    def quote_param(self, param):
        return {k:self.quote_value(v) for k, v in param.items()}

    #convert values to mysql code for the same
    #mostly delegate directly to the mysql lib, but some exceptions exist
    def quote_value(self, value):
        try:
            if isinstance(value, basestring):
                return self.db.literal(value)
            elif isinstance(value, datetime):
                return "str_to_date('"+value.strftime("%Y%m%d%H%M%S")+"', '%Y%m%d%H%i%s')"
            elif isinstance(value, list):
                return "("+",".join([self.db.literal(vv) for vv in value])+")"
            elif isinstance(value, SQL):
                return value.value
            elif isinstance(value, dict):
                return self.db.literal(None)
            elif Math.is_number(value):
                return unicode(value)
            else:
                return self.db.literal(value)
        except Exception, e:
            D.error("problem quoting SQL", e)


    def quote_column(self, column_name):
        return "`"+column_name.replace(".", "`.`")+"`"    #MY SQL QUOTE OF COLUMN NAMES



def utf8_to_unicode(v):
    try:
        if isinstance(v, str):
            return v.decode("utf8")
        else:
            return v
    except Exception, e:
        D.error("not expected", e)

        
#ACTUAL SQL, DO NOT QUOTE THIS STRING
class SQL():


    def __init__(self, template='', param=None):
        self.value=expand_template(template, param)

    def __str__(self):
        return self.value