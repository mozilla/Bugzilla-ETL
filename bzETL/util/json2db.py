
# schema required for types and nested types

# only need to know the nested, and the mutli-valued
import string
import struct
from .strings import expand_template
from .db import DB, SQL
from .logs import Log
from .struct import Struct


OBJECT="object" # USED IN property.type TO INDICATE FURTHER STRUCTURE
NESTED="nested" # USED TO INDICATE column.type IS AN OBJECT
MULTI="multi"  # True IF multi-valued column, "ordered" if multivalued and ordered (has index)
ORDERED="ordered"
INDEX="index"  # True, "yes", "not_analyzed", <PARSER NAME> to indicate the for of text parsing used

NOT_ANALYZED="not_analyzed"

PRIMITIVES=struct.wrap({
    "string":{"sql_type":"VARCHAR(100)"},
    "integer":{"sql_type":"LONG"},
    "float":{"sql_type":"DOUBLE"},
    "boolean":{"sql_type":"DECIMAL(1)"}
})
TYPES=[OBJECT, "nested"]


PARSERS = {
    True: simple_words,
    "yes": simple_words,
    NOT_ANALYZED: no_parse,

}

def simple_words(value):
    return value.split(" ")

def no_parse(value):
    return value.trim()


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

    def start(self, settings):
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
            if not c[MULTI] and c.type not in TYPES:
                desc.append(self.db.quote_column(prefix+c.name) + " " + PRIMITIVES[c.type].sql_type)
            elif c.type == OBJECT:
                self.add_columns(prefix+c.name+".", desc, c.columns)


    def build_schema(self, schema, path):
        """
        for storing the json, indexing should be done in separate indexed
        tables

        free-form text should have index and stored value separate
        index = (trigram, ref)
        content = (ref, content)
        """



        # schema has name and columns
        # path is fullpath of hierarchy
        assert schema.name == path[-1]

        desc=[]
        if schema.multi:
            desc.append("_id INTEGER PRIMARY KEY")

        if schema.multi==ORDERED:
            desc.append(expand_template("_parent INTEGER REFERENCES {{parent}}(_id)", {
                "parent":self.db.quote_column(".".join(path[:-1]))
            }))

        if schema.type in PRIMITIVES:
            desc.append("_value " + PRIMITIVES[schema.type].sql_type)

        self.add_columns("", desc, schema.columns)

        self.db.execute("""
            CREATE TABLE {{table_name}} (
                {{columns}}
            )
        """, {
            "table_name":self.db.quote_column(path),
            "columns":SQL(desc)
        })

        #MAKE CHILD TABLES
        for c in schema.columns:
            if c.type=="nested" or c.multi:
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


    def enhance_schema(self, path, json, schema):
        #LOOK FOR NEW PRIMITIVE ATTRIBUTES
        #PRIMITIVE TO MULTIVALUED
        #OBJECT TO NESTED

        for k, v in json.items():
            full_path = "."[path, k.replace(".", "\.")]

            if not schema[k]:
                column_def = self.defaults.getSchema(full_path, example=v)  # PATTERN MATCHING SCHEMA GENERATOR
                # ALTER TABLE TO HANDLE NEW SCHEMA

                desc=[]  #ADDED COLUMN DEFINITIONS
                self.add_columns(path, desc, [column_def])
                self.build_schema(column_def, full_path)
                self.db.execute("ALTER TABLE {{table_name}} ({{details}}", {
                    "table_name": self.db.quote_column(path),
                    "details": "\n".join(desc)
                })
            elif isinstance(v, list):
                if schema[k].type==MULTI:
                    self.enhance_schema(full_path, v, schema[k])
                elif schema[k].type==NESTED:
                    self.enhance_schema(full_path, v, schema[k])
                else:
                    #MOVE FROM PRIMITIVE TO MULTI
                    #MOVE FROM OBJECT TO NESTED
            elif isinstance(v, dict) and not schema[k].type==OBJECT:




    def _add(self, json, type_name, type_info):
        #ENSURE THE json FITS IN THE SCHEMA


        # FIND NESTED FIELDS

        record=Struct()
        for key, desc in type_info:
            if desc.type in PRIMITIVES:
                record[key]=json[key]
            elif desc.type[-2:]=="[]":
                self._add(json[key], type_name+"."+key, desc)


        self.db.insert(type_name, record)


