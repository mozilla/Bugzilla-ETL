################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################


from datetime import datetime
from pymysql import connect
from util.strings import expand_template
from util.basic import nvl
from util.cnv import CNV
from util.debug import D
from util.struct import Struct
from util.query import Q
from util.strings import indent
from util.strings import outdent
from util.files import File



DEBUG = False
MAX_BATCH_SIZE=100

class DB():

    def __init__(self, settings):
        if isinstance(settings, DB):
            settings=settings.settings

        self.settings=settings
        try:
            self.db=connect(
                host=settings.host,
                port=settings.port,
                user=nvl(settings.username, settings.user),
                passwd=nvl(settings.password, settings.passwd),
                db=nvl(settings.schema, settings.db),
                charset="utf8",
                use_unicode=True
            )
        except Exception, e:
            D.error("Failure to connect", e)
        self.cursor=None
        self.partial_rollback=False
        self.transaction_level=0
        self.debug=settings.debug is not None or DEBUG
        self.backlog=[]     #accumulate the write commands so they are sent at once


    def __enter__(self):
        self.begin()
        return self

    def __exit__(self, type, value, traceback):
        try:
            self.commit()
        except Exception, e:
            self.rollback()
            D.error("Problems while connected to db", e)
        finally:
            self.close()


    def begin(self):
        if self.transaction_level==0: self.cursor=self.db.cursor()
        self.transaction_level+=1

    def close(self):
        if self.transaction_level>0:
            D.error("expecting commit() or rollback() before close")
        self.cursor=None  #NOT NEEDED
        self.db.close()

    def commit(self):
        try:
            self.execute_backlog()
        except Exception, e:
            D.error("Unexpected error", e)
            
        if self.transaction_level==0:
            D.error("No transaction has begun")
        elif self.transaction_level==1:
            if self.partial_rollback:
                D.warning("Commit after nested rollback is not allowed")
                self.db.rollback()
            else:
                if self.cursor: self.cursor.close()
                self.cursor=None
                self.db.commit()

        self.transaction_level-=1

    def flush(self):
        self.commit()
        self.begin()

    def rollback(self):
        self.backlog=[]     #YAY! FREE!
        if self.transaction_level==0:
            D.error("No transaction has begun")
        elif self.transaction_level==1:
            if self.cursor: self.cursor.close()
            self.cursor=None
            self.db.rollback()
        else:
            self.partial_rollback=True
            D.warning("Can not perform partial rollback!")
        self.transaction_level-=1


    def call(self, proc_name, params):
        self.execute_backlog()
        try:
            self.cursor.callproc(proc_name, params)
            self.cursor.close()
            self.cursor=self.db.cursor()
        except Exception, e:
            D.error("Problem calling procedure "+proc_name, e)



    def query(self, sql, param=None):
        self.execute_backlog()
        try:
            old_cursor=self.cursor
            if old_cursor is None: #ALLOW NON-TRANSACTIONAL READS
                self.cursor=self.db.cursor()

            if param is not None: sql=expand_template(sql, self.quote(param))
            sql=outdent(sql)
            if self.debug: D.println("Execute SQL:\n"+indent(sql))

            self.cursor.execute(sql)

            columns = tuple( [d[0].decode('utf8') for d in self.cursor.description] )
            result=CNV.table2list(columns, self.cursor)

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

        self.execute_backlog()
        try:
            old_cursor=self.cursor
            if old_cursor is None: #ALLOW NON-TRANSACTIONAL READS
                self.cursor=self.db.cursor()

            if param is not None: sql=expand_template(sql,self.quote(param))
            sql=outdent(sql)
            if self.debug: D.println("Execute SQL:\n"+indent(sql))

            self.cursor.execute(sql)

            columns = tuple( [d[0].decode('utf8') for d in self.cursor.description] )
            for r in self.cursor:
                num+=1
                execute(Struct(**dict(zip(columns, r))))

            if old_cursor is None:   #CLEANUP AFTER NON-TRANSACTIONAL READS
                self.cursor.close()
                self.cursor=None

        except Exception, e:
            D.error("Problem executing SQL:\n"+indent(sql.strip()), e, offset=1)

        return num

    
    def execute(self, sql, param=None):
        if self.transaction_level==0: D.error("Expecting transation to be started before issuing queries")

        if param is not None: sql=expand_template(sql,self.quote(param))
        sql=outdent(sql)
        self.backlog.append(sql)
        if len(self.backlog)>=MAX_BATCH_SIZE:
            self.execute_backlog()

        
    def execute_file(self, filename, param=None):
        content=File(filename).read()
        self.execute(content, param)

    @staticmethod
    def execute_sql(settings, sql, param=None):
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


    def execute_backlog(self):
        if len(self.backlog)==0: return

        for i, g in Q.groupby(self.backlog, size=MAX_BATCH_SIZE):
            sql=";\n".join(g)
            try:
                if self.debug: D.println("Execute block of SQL:\n"+indent(sql))
                self.cursor.execute(sql)
                self.cursor.close()
                self.cursor = self.db.cursor()
            except Exception, e:
                D.error("Problem executing SQL:\n{{sql}}", {"sql":indent(sql.strip())}, e, offset=1)

        self.backlog=[]



    ## Insert dictionary of values into table
    def insert (self, table_name, param):
        def quote(value):
            return "`"+value+"`"    #MY SQL QUOTE OF COLUMN NAMES

        keys = param.keys()
        param = self.quote(param)

        command = "INSERT INTO "+quote(table_name)+"("+\
                  ",".join([quote(k) for k in keys])+\
                  ") VALUES ("+\
                  ",".join([param[k] for k in keys])+\
                  ")"

        self.execute(command)


    def insert_list(self, table_name, list):
        #PROBABLY CAN BE BETTER DONE WITH executeMany()
        for l in list:
            self.insert(table_name, l)


    def update(self, table_name, where, new_values):
        def quote(value):
            return "`"+value+"`"    #MY SQL QUOTE OF COLUMN NAMES

        where=self.quote(where)
        new_values=self.quote(new_values)

        command="UPDATE "+quote(table_name)+"\n"+\
                "SET "+\
                ",\n".join([quote(k)+"="+v for k,v in new_values.items()])+"\n"+\
                "WHERE "+\
                " AND\n".join([quote(k)+"="+v for k,v in where.items()])
        self.execute(command, {})


    #convert values to mysql code for the same
    #mostly delegate directly to the mysql lib, but some exceptions exist
    def quote(self, param):
        try:
            output={}
            for k, v in [(k, param[k]) for k in param.keys()]:
                if isinstance(v, datetime):
                    v=SQL("str_to_date('"+v.strftime("%Y%m%d%H%M%S")+"', '%Y%m%d%H%i%s')")
                elif isinstance(v, list):
                    v=SQL("("+",".join([self.db.literal(vv) for vv in v])+")")
                elif isinstance(v, SQL):
                    pass
                elif isinstance(v, Struct):
                    self.db.literal(None)
                else:
                    v=SQL(self.db.literal(v))

                output[k]=v
            return output
        except Exception, e:
            D.error("problem quoting SQL", e)




#ACTUAL SQL, DO NOT QUOTE THIS STRING
class SQL(str):

    def __init__(self, string=''):
        str.__init__(self, string)

    def __str__(self):
        return self