
# schema required for types and nested types

# only need to know the nested, and the mutli-valued
from dzAlerts.util.db import DB
from dzAlerts.util.logs import Log
from dzAlerts.util.query import Q
from dzAlerts.util.struct import Struct


PRIMITIVES=["string", "integer", "float", "boolean"]


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

    def build_schema(self, schema, path, item):

        assert schema.name.startswith(path[0])

        if len(path)==1:
            schema.columns.path=item
            return






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


