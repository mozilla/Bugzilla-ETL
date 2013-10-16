
# schema required for types and nested types

# only need to know the nested, and the mutli-valued
from msilib import schema
from .db import DB, SQL
from .logs import Log
from .struct import Struct


PRIMITIVES={
    "string":{"sql_type":"VARCHAR(100)"},
    "integer":{"sql_type":"LONG"},
    "float":{"sql_type":"DOUBLE"},
    "boolean":{"sql_type":"DECIMAL(1)"}
}
TYPES=["object", "nested"]


class indexed():

    def __init__(self, settings):
        self.db=DB(settings.database)
        self.settings=settings
        self.index_name=settings.index



    def __enter__(self):
        self.db=DB(self.settings.database)
        return self

    def __exit__(self, type, value, traceback):
        if isinstance(value, BaseException):
            self.db.rollback()
            self.db.close()
            return

        try:
            self.db.commit()
        except Exception, e:
            Log.warning(u"can not commit()", e)
        finally:
            self.db.close()



    def setup(self):
        self.db.execute("""
            CREATE TABLE `info.schema` (
                path        VARCHAR(300),
                type        VARCHAR (30),
                `index`     DECIMAL(1)
            )
        """)

    def start(settings):
        schema_list=self.db.query("""
            SELECT
                path,
                type,
                `index`
            FROM
                `info.schema`
        """)

    def add_columns(self, prefix, desc, columns):
        for c in columns:
            if c.type not in TYPES:
                desc.append(self.db.quote_column(prefix+c.name) + " " + PRIMITIVES[c.type].sql_type)
            elif c.type == "object":
                self.add_columns(prefix+c.name+".", desc, c.columns)


    def build_schema(self, schema, path):
        """
        for storing the json, indexing should be done in separate indexed
        tables

        free-form text should have index and stored value seperate
        index = (trigram, ref)
        content =

        """



        # schema has name and columns
        # path is fullpath of hierarchy
        assert schema.name == path[-1]

        desc=[]
        self.add_columns("", desc, schema.columns)

        for c in schema.columns:
            if c.type=="nested":
                self.build_schema(c, path+[schema.name])

        # BUILD TABLE
        self.db.execute("""
            CREATE TABLE {{table_name}} (
                {{columns}}
            )
        """, {
            "table_name":self.db.quote_column(".".join(schema.path)),
            "columns":SQL("\n".join(desc))

        })





    def enhance_schema(self, path, type):



    def _add(self, json, type_name, type_info):
        # FIND NESTED FIELDS

        record=Struct()
        for key, desc in type_info:
            if desc.type in PRIMITIVES:
                record[key]=json[key]
            elif desc.type[-2:]=="[]":
                self._add(json[key], type_name+"."+key, desc)


        self.db.insert(type_name, record)


