# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#




from collections import Mapping

from pyLibrary import convert
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import Dict, wrap, listwrap, unwraplist, DictList, unwrap, set_default, join_field, split_field
from pyLibrary.queries import jx
from pyLibrary.queries.containers import Container
from pyLibrary.queries.expression_compiler import compile_expression
from pyLibrary.queries.expressions import TRUE_FILTER, jx_expression, Expression, TrueOp, jx_expression_to_function, Variable
from pyLibrary.queries.lists.aggs import is_aggs, list_aggs
from pyLibrary.queries.meta import Column
from pyLibrary.thread.threads import Lock
from pyLibrary.times.dates import Date

_get = object.__getattribute__


class ListContainer(Container):
    def __init__(self, name, data, schema=None):
        #TODO: STORE THIS LIKE A CUBE FOR FASTER ACCESS AND TRANSFORMATION
        data = list(unwrap(data))
        Container.__init__(self, data, schema)
        if schema == None:
            self.schema = get_schema_from_list(data)
        else:
            self.schema = schema
        self.name = name
        self.data = data
        self.locker = Lock()  # JUST IN CASE YOU WANT TO DO MORE THAN ONE THING

    @property
    def query_path(self):
        return None

    def query(self, q):
        frum = self
        if is_aggs(q):
            frum = list_aggs(frum.data, q)
        else:  # SETOP
            try:
                if q.filter != None or q.esfilter != None:
                    Log.error("use 'where' clause")
            except AttributeError as e:
                pass

            if q.where is not TRUE_FILTER and not isinstance(q.where, TrueOp):
                frum = frum.filter(q.where)

            if q.sort:
                frum = frum.sort(q.sort)

            if q.select:
                frum = frum.select(q.select)
        #TODO: ADD EXTRA COLUMN DESCRIPTIONS TO RESULTING SCHEMA
        for param in q.window:
            frum.window(param)

        return frum

    def update(self, command):
        """
        EXPECTING command == {"set":term, "clear":term, "where":where}
        THE set CLAUSE IS A DICT MAPPING NAMES TO VALUES
        THE where CLAUSE IS A JSON EXPRESSION FILTER
        """
        command = wrap(command)
        command_clear = listwrap(command["clear"])
        command_set = list(command.set.items())
        command_where = jx.get(command.where)

        for c in self.data:
            if command_where(c):
                for k in command_clear:
                    c[k] = None
                for k, v in command_set:
                    c[k] = v

    def filter(self, where):
        return self.where(where)

    def where(self, where):
        temp = None
        if isinstance(where, Mapping):
            exec("def temp(row):\n    return "+jx_expression(where).to_python())
        elif isinstance(where, Expression):
            temp = compile_expression(where.to_python())
        else:
            temp = where

        return ListContainer("from "+self.name, list(filter(temp, self.data)), self.schema)

    def sort(self, sort):
        return ListContainer("from "+self.name, jx.sort(self.data, sort, already_normalized=True), self.schema)

    def get(self, select):
        """
        :param select: the variable to extract from list
        :return:  a simple list of the extraction
        """
        if isinstance(select, list):
            return [(d[s] for s in select) for d in self.data]
        else:
            return [d[select] for d in self.data]

    def select(self, select):
        selects = listwrap(select)

        if len(selects) == 1 and isinstance(selects[0].value, Variable) and selects[0].value.var == ".":
            new_schema = self.schema
            if selects[0].name == ".":
                return self
        else:
            new_schema = None

        if isinstance(select, list):
            push_and_pull = [(s.name, jx_expression_to_function(s.value)) for s in selects]
            def selector(d):
                output = Dict()
                for n, p in push_and_pull:
                    output[n] = p(wrap(d))
                return unwrap(output)

            new_data = list(map(selector, self.data))
        else:
            select_value = jx_expression_to_function(select.value)
            new_data = list(map(select_value, self.data))

        return ListContainer("from "+self.name, data=new_data, schema=new_schema)

    def window(self, window):
        _ = window
        jx.window(self.data, window)
        return self

    def having(self, having):
        _ = having
        Log.error("not implemented")

    def format(self, format):
        if format == "table":
            frum = convert.list2table(self.data, list(self.schema.keys()))
        elif format == "cube":
            frum = convert.list2cube(self.data, list(self.schema.keys()))
        else:
            frum = self.to_dict()

        return frum

    def insert(self, documents):
        self.data.extend(documents)

    def extend(self, documents):
        self.data.extend(documents)

    def add(self, doc):
        self.data.append(doc)

    def to_dict(self):
        return wrap({
            "meta": {"format": "list"},
            "data": [{k: unwraplist(v) for k, v in list(row.items())} for row in self.data]
        })

    def get_columns(self, table_name=None):
        return list(self.schema.values())

    def __getitem__(self, item):
        return self.data[item]

    def __iter__(self):
        return self.data.__iter__()

    def __len__(self):
        return len(self.data)

def get_schema_from_list(frum):
    """
    SCAN THE LIST FOR COLUMN TYPES
    """
    columns = {}
    _get_schema_from_list(frum, columns, prefix=[], nested_path=["."])
    return columns

def _get_schema_from_list(frum, columns, prefix, nested_path):
    """
    SCAN THE LIST FOR COLUMN TYPES
    """
    names = {}
    for d in frum:
        row_type = _type_to_name[d.__class__]
        if row_type!="object":
            agg_type = names.get(".", "undefined")
            names["."] = _merge_type[agg_type][row_type]
        else:
            for name, value in list(d.items()):
                agg_type = names.get(name, "undefined")
                if isinstance(value, list):
                    if len(value)==0:
                        this_type = "undefined"
                    else:
                        this_type=_type_to_name[value[0].__class__]
                        if this_type=="object":
                            this_type="nested"
                else:
                    this_type = _type_to_name[value.__class__]
                new_type = _merge_type[agg_type][this_type]
                names[name] = new_type

                if this_type == "object":
                    _get_schema_from_list([value], columns, prefix + [name], nested_path)
                elif this_type == "nested":
                    np = listwrap(nested_path)
                    newpath = unwraplist([join_field(split_field(np[0])+[name])]+np)
                    _get_schema_from_list(value, columns, prefix + [name], newpath)

    for n, t in list(names.items()):
        full_name = ".".join(prefix + [n])
        column = Column(
            name=full_name,
            table=".",
            es_column=full_name,
            es_index=".",
            type=t,
            nested_path=nested_path
        )
        columns[column.name] = column


_type_to_name = {
    None: "undefined",
    str: "string",
    str: "string",
    int: "integer",
    int: "long",
    float: "double",
    Dict: "object",
    dict: "object",
    set: "nested",
    list: "nested",
    DictList: "nested",
    Date: "double"
}

_merge_type = {
    "undefined": {
        "boolean": "boolean",
        "integer": "integer",
        "long": "long",
        "float": "float",
        "double": "double",
        "string": "string",
        "object": "object",
        "nested": "nested"
    },
    "boolean": {
        "boolean": "boolean",
        "integer": "integer",
        "long": "long",
        "float": "float",
        "double": "double",
        "string": "string",
        "object": None,
        "nested": None
    },
    "integer": {
        "boolean": "integer",
        "integer": "integer",
        "long": "long",
        "float": "float",
        "double": "double",
        "string": "string",
        "object": None,
        "nested": None
    },
    "long": {
        "boolean": "long",
        "integer": "long",
        "long": "long",
        "float": "double",
        "double": "double",
        "string": "string",
        "object": None,
        "nested": None
    },
    "float": {
        "boolean": "float",
        "integer": "float",
        "long": "double",
        "float": "float",
        "double": "double",
        "string": "string",
        "object": None,
        "nested": None
    },
    "double": {
        "boolean": "double",
        "integer": "double",
        "long": "double",
        "float": "double",
        "double": "double",
        "string": "string",
        "object": None,
        "nested": None
    },
    "string": {
        "boolean": "string",
        "integer": "string",
        "long": "string",
        "float": "string",
        "double": "string",
        "string": "string",
        "object": None,
        "nested": None
    },
    "object": {
        "boolean": None,
        "integer": None,
        "long": None,
        "float": None,
        "double": None,
        "string": None,
        "object": "object",
        "nested": "nested"
    },
    "nested": {
        "boolean": None,
        "integer": None,
        "long": None,
        "float": None,
        "double": None,
        "string": None,
        "object": "nested",
        "nested": "nested"
    }
}



def _exec(code):
    try:
        temp = None
        exec("temp = " + code)
        return temp
    except Exception as e:
        Log.error("Could not execute {{code|quote}}", code=code, cause=e)
