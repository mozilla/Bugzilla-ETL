# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals
from datetime import datetime
import re
import time
import requests

from ..maths.randoms import Random
from ..thread.threads import ThreadedQueue
from ..maths import Math
from ..cnv import CNV
from ..env.logs import Log
from ..struct import nvl, Null, wrap, unwrap
from ..struct import Struct, StructList


DEBUG = False


class ElasticSearch(object):
    """
    AN ElasticSearch INDEX LIFETIME MANAGEMENT TOOL

    ElasticSearch'S REST INTERFACE WORKS WELL WITH PYTHON AND JAVASCRIPT
    SO HARDLY ANY LIBRARY IS REQUIRED.  IT IS SIMPLER TO MAKE HTTP CALLS
    DIRECTLY TO ES USING YOUR FAVORITE HTTP LIBRARY.  I HAVE SOME
    CONVENIENCE FUNCTIONS HERE, BUT IT'S BETTER TO MAKE YOUR OWN.

    THIS CLASS IS TO HELP DURING ETL, CREATING INDEXES, MANAGING ALIASES
    AND REMOVING INDEXES WHEN THEY HAVE BEEN REPLACED.  IT USES A STANDARD
    SUFFIX (YYYYMMDD-HHMMSS) TO TRACK AGE AND RELATIONSHIP TO THE ALIAS,
    IF ANY YET.

    """
    def __init__(self, settings=None):
        if settings is None:
            self.debug = DEBUG
            return

        settings = wrap(settings)
        assert settings.host
        assert settings.index
        assert settings.type

        if settings.index == settings.alias:
            Log.error("must have a unique index name")
        self.cluster_metadata = None
        if not settings.port:
            settings.port = 9200
        self.debug = nvl(settings.debug, DEBUG)
        self.settings = settings
        index = self.get_index(settings.index)
        if index:
            settings.alias = settings.index
            settings.index = index

        self.path = settings.host + ":" + unicode(settings.port) + "/" + settings.index + "/" + settings.type



    @staticmethod
    def create_index(settings, schema, limit_replicas=False):
        schema = wrap(schema)
        if isinstance(schema, basestring):
            schema = CNV.JSON2object(schema)

        if limit_replicas:
            # DO NOT ASK FOR TOO MANY REPLICAS
            health = DUMMY.get(settings.host + ":" + unicode(settings.port) + "/_cluster/health")
            if schema.settings.index.number_of_replicas >= health.number_of_nodes:
                Log.warning("Reduced number of replicas: {{from}} requested, {{to}} realized", {
                    "from": schema.settings.index.number_of_replicas,
                    "to": health.number_of_nodes-1
                })
                schema.settings.index.number_of_replicas = health.number_of_nodes-1

        DUMMY.post(
            settings.host + ":" + unicode(settings.port) + "/" + settings.index,
            data=CNV.object2JSON(schema).encode("utf8"),
            headers={"Content-Type": "application/json"}
        )
        time.sleep(2)
        es = ElasticSearch(settings)
        return es

    @staticmethod
    def delete_index(settings, index=None):
        index = nvl(index, settings.index)

        DUMMY.delete(
            settings.host + ":" + unicode(settings.port) + "/" + index,
        )

    def get_aliases(self):
        """
        RETURN LIST OF {"alias":a, "index":i} PAIRS
        ALL INDEXES INCLUDED, EVEN IF NO ALIAS {"alias":Null}
        """
        data = self.get_metadata().indices
        output = []
        for index, desc in data.items():
            if not desc["aliases"]:
                output.append({"index": index, "alias": None})
            else:
                for a in desc["aliases"]:
                    output.append({"index": index, "alias": a})
        return wrap(output)

    def get_metadata(self):
        if not self.cluster_metadata:
            response = self.get(self.settings.host + ":" + unicode(self.settings.port) + "/_cluster/state")
            self.cluster_metadata = response.metadata
            self.node_metatdata = self.get(self.settings.host + ":" + unicode(self.settings.port) + "/")
        return self.cluster_metadata

    def get_schema(self):
        indices = self.get_metadata().indices
        index = indices[self.settings.index]
        if not index.mappings[self.settings.type]:
            Log.error("{{index}} does not have type {{type}}", self.settings)
        return index.mappings[self.settings.type]

    #DELETE ALL INDEXES WITH GIVEN PREFIX, EXCEPT name
    def delete_all_but(self, prefix, name):
        if prefix == name:
            Log.note("{{index_name}} will not be deleted", {"index_name": prefix})
        for a in self.get_aliases():
            # MATCH <prefix>YYMMDD_HHMMSS FORMAT
            if re.match(re.escape(prefix) + "\\d{8}_\\d{6}", a.index) and a.index != name:
                ElasticSearch.delete_index(self.settings, a.index)

    @staticmethod
    def proto_name(prefix, timestamp=None):
        if not timestamp:
            timestamp = datetime.utcnow()
        return prefix + CNV.datetime2string(timestamp, "%Y%m%d_%H%M%S")

    def add_alias(self, alias):
        self.cluster_metadata = None
        requests.post(
            self.settings.host + ":" + unicode(self.settings.port) + "/_aliases",
            CNV.object2JSON({
                "actions": [
                    {"add": {"index": self.settings.index, "alias": alias}}
                ]
            }),
            timeout=nvl(self.settings.timeout, 30)
        )

    def get_proto(self, alias):
        """
        RETURN ALL INDEXES THAT ARE INTENDED TO BE GIVEN alias, BUT HAVE NO
        ALIAS YET BECAUSE INCOMPLETE
        """
        output = sort([
            a.index
            for a in self.get_aliases()
            if re.match(re.escape(alias) + "\\d{8}_\\d{6}", a.index) and not a.alias
        ])
        return output

    def get_index(self, alias):
        """
        RETURN THE INDEX USED BY THIS alias
        """
        output = sort([
            a.index
            for a in self.get_aliases()
            if a.alias == alias
        ])
        if len(output) > 1:
            Log.error("only one index with given alias==\"{{alias}}\" expected", {"alias": alias})

        if not output:
            return Null

        return output.last()

    def is_proto(self, index):
        """
        RETURN True IF THIS INDEX HAS NOT BEEN ASSIGNED ITS ALIAS
        """
        for a in self.get_aliases():
            if a.index == index and a.alias:
                return False
        return True

    def delete_record(self, filter):
        self.get_metadata()
        if self.node_metatdata.version.number.startswith("0.90"):
            query = filter
        elif self.node_metatdata.version.number.startswith("1.0"):
            query = {"query": filter}
        else:
            Log.error("not implemented yet")

        if self.debug:
            Log.note("Delete bugs:\n{{query}}", {"query": query})

        self.delete(
            self.path + "/_query",
            data=CNV.object2JSON(query)
        )

    def extend(self, records):
        """
        records - MUST HAVE FORM OF
            [{"value":value}, ... {"value":value}] OR
            [{"json":json}, ... {"json":json}]
            OPTIONAL "id" PROPERTY IS ALSO ACCEPTED
        """
        lines = []
        try:
            for r in records:
                id = r.get("id", None)
                if "json" in r:
                    json = r["json"]
                elif "value" in r:
                    json = CNV.object2JSON(r["value"])
                else:
                    Log.error("Expecting every record given to have \"value\" or \"json\" property")

                if id == None:
                    id = Random.hex(40)

                lines.append('{"index":{"_id": ' + CNV.object2JSON(id) + '}}')
                lines.append(json)

            if not lines:
                return
            response = self.post(
                self.path + "/_bulk",
                data=("\n".join(lines) + "\n").encode("utf8"),
                headers={"Content-Type": "text"},
                timeout=self.settings.timeout
            )
            items = response["items"]

            for i, item in enumerate(items):
                if not item.index.ok:
                    Log.error("{{error}} while loading line:\n{{line}}", {
                        "error": item.index.error,
                        "line": lines[i * 2 + 1]
                    })

            if self.debug:
                Log.note("{{num}} items added", {"num": len(lines) / 2})
        except Exception, e:
            if e.message.startswith("sequence item "):
                Log.error("problem with {{data}}", {"data": repr(lines[int(e.message[14:16].strip())])}, e)
            Log.error("problem", e)


    # RECORDS MUST HAVE id AND json AS A STRING OR
    # HAVE id AND value AS AN OBJECT
    def add(self, record):
        if isinstance(record, list):
            Log.error("add() has changed to only accept one record, no lists")
        self.extend([record])

    # -1 FOR NO REFRESH
    def set_refresh_interval(self, seconds):
        if seconds <= 0:
            interval = "-1"
        else:
            interval = unicode(seconds) + "s"

        response = self.put(
            self.settings.host + ":" + unicode(
                self.settings.port) + "/" + self.settings.index + "/_settings",
            data="{\"index.refresh_interval\":\"" + interval + "\"}"
        )

        result = CNV.JSON2object(response.content)
        if not result.ok:
            Log.error("Can not set refresh interval ({{error}})", {
                "error": response.content
            })

    def search(self, query):
        query = wrap(query)
        try:
            if self.debug:
                if len(query.facets.keys()) > 20:
                    show_query = query.copy()
                    show_query.facets = {k: "..." for k in query.facets.keys()}
                else:
                    show_query = query
                Log.note("Query:\n{{query|indent}}", {"query": show_query})
            return self.post(
                self.path + "/_search",
                data=CNV.object2JSON(query).encode("utf8"),
                timeout=self.settings.timeout
            )
        except Exception, e:
            Log.error("Problem with search (path={{path}}):\n{{query|indent}}", {
                "path": self.path + "/_search",
                "query": query
            }, e)

    def threaded_queue(self, size=None, period=None):
        return ThreadedQueue(self, size=size, period=period)

    def post(self, *args, **kwargs):
        if "data" in kwargs and not isinstance(kwargs["data"], str):
            Log.error("data must be utf8 encoded string")

        try:
            kwargs = wrap(kwargs)
            kwargs.setdefault("timeout", 600)
            kwargs.headers["Accept-Encoding"] = "gzip,deflate"
            kwargs = unwrap(kwargs)
            response = requests.post(*args, **kwargs)
            if self.debug:
                Log.note(response.content[:130])
            details = CNV.JSON2object(response.content)
            if details.error:
                Log.error(CNV.quote2string(details.error))
            if details._shards.failed > 0:
                Log.error("Shard failure")
            return details
        except Exception, e:
            if args[0][0:4] != "http":
                suggestion = " (did you forget \"http://\" prefix on the host name?)"
            else:
                suggestion = ""

            Log.error("Problem with call to {{url}}" + suggestion +"\n{{body}}", {
                "url": args[0],
                "body": kwargs["data"] if DEBUG else kwargs["data"][0:100]
            }, e)

    def get(self, *args, **kwargs):
        try:
            kwargs = wrap(kwargs)
            kwargs.setdefault("timeout", 600)
            response = requests.get(*args, **kwargs)
            if self.debug:
                Log.note(response.content[:130])
            details = wrap(CNV.JSON2object(response.content))
            if details.error:
                Log.error(details.error)
            return details
        except Exception, e:
            Log.error("Problem with call to {{url}}", {"url": args[0]}, e)

    def put(self, *args, **kwargs):
        try:
            kwargs = wrap(kwargs)
            kwargs.setdefault("timeout", 30)
            response = requests.put(*args, **kwargs)
            if self.debug:
                Log.note(response.content)
            return response
        except Exception, e:
            Log.error("Problem with call to {{url}}", {"url": args[0]}, e)

    def delete(self, *args, **kwargs):
        try:
            kwargs.setdefault("timeout", 30)
            response = requests.delete(*args, **kwargs)
            if self.debug:
                Log.note(response.content)
            return response
        except Exception, e:
            Log.error("Problem with call to {{url}}", {"url": args[0]}, e)

    @staticmethod
    def scrub(r):
        """
        REMOVE KEYS OF DEGENERATE VALUES (EMPTY STRINGS, EMPTY LISTS, AND NULLS)
        TO LOWER CASE
        CONVERT STRINGS OF NUMBERS TO NUMBERS
        RETURNS **COPY**, DOES NOT CHANGE ORIGINAL
        """
        return wrap(_scrub(r))


def _scrub(r):
    try:
        if r == None:
            return None
        elif isinstance(r, basestring):
            if r == "":
                return None
            return r
        elif Math.is_number(r):
            return CNV.value2number(r)
        elif isinstance(r, dict):
            if isinstance(r, Struct):
                r = object.__getattribute__(r, "__dict__")
            output = {}
            for k, v in r.items():
                v = _scrub(v)
                if v != None:
                    output[k.lower()] = v
            if len(output) == 0:
                return None
            return output
        elif hasattr(r, '__iter__'):
            if isinstance(r, StructList):
                r = r.list
            output = []
            for v in r:
                v = _scrub(v)
                if v != None:
                    output.append(v)
            if not output:
                return None
            try:
                return sort(output)
            except Exception:
                return output
        else:
            return r
    except Exception, e:
        Log.warning("Can not scrub: {{json}}", {"json": r})



def sort(values):
    return wrap(sorted(values))


DUMMY = ElasticSearch()
