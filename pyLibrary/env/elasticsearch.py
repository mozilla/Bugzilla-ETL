# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals
from __future__ import division
from copy import deepcopy

from datetime import datetime
import re
import time
import requests

from ..collections import OR
from ..cnv import CNV
from ..env.logs import Log
from ..maths.randoms import Random
from ..maths import Math
from ..strings import utf82unicode
from ..struct import nvl, Null
from ..structs.wraps import wrap, unwrap
from ..struct import Struct, StructList
from ..thread.threads import ThreadedQueue


DEBUG = False


class Index(object):
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

    def __init__(self, settings):
        """
        settings.explore_metadata == True - IF PROBING THE CLUSTER FOR METATDATA IS ALLOWED
        settings.timeout == NUMBER OF SECONDS TO WAIT FOR RESPONSE, OR SECONDS TO WAIT FOR DOWNLOAD (PASSED TO requests)
        """
        if settings.index == settings.alias:
            Log.error("must have a unique index name")

        settings = wrap(settings)
        assert settings.index
        assert settings.type
        settings.setdefault("explore_metadata", True)

        self.debug = nvl(settings.debug, DEBUG)
        globals()["DEBUG"] = OR(self.debug, DEBUG)
        if self.debug:
            Log.note("elasticsearch debugging is on")

        self.settings = settings
        self.cluster = Cluster(settings)

        try:
            index = self.get_index(settings.index)
            if index:
                settings.alias = settings.index
                settings.index = index
        except Exception, e:
            # EXPLORING (get_metadata()) IS NOT ALLOWED ON THE PUBLIC CLUSTER
            pass

        self.path = "/" + settings.index + "/" + settings.type


    def get_schema(self):
        if self.settings.explore_metadata:
            indices = self.cluster.get_metadata().indices
            index = indices[self.settings.index]
            if not index.mappings[self.settings.type]:
                Log.error("ElasticSearch index ({{index}}) does not have type ({{type}})", self.settings)
            return index.mappings[self.settings.type]
        else:
            mapping = self.cluster.get(self.path + "/_mapping")
            if not mapping[self.settings.type]:
                Log.error("{{index}} does not have type {{type}}", self.settings)
            return wrap({"mappings": mapping[self.settings.type]})

    def delete_all_but_self(self):
        """
        DELETE ALL INDEXES WITH GIVEN PREFIX, EXCEPT name
        """
        prefix = self.settings.alias
        name = self.settings.index

        if prefix == name:
            Log.note("{{index_name}} will not be deleted", {"index_name": prefix})
        for a in self.cluster.get_aliases():
            # MATCH <prefix>YYMMDD_HHMMSS FORMAT
            if re.match(re.escape(prefix) + "\\d{8}_\\d{6}", a.index) and a.index != name:
                self.cluster.delete_index(a.index)

    def add_alias(self):
        self.cluster_metadata = None
        self.cluster._post(
            "/_aliases",
            CNV.object2JSON({
                "actions": [
                    {"add": {"index": self.settings.index, "alias": self.settings.alias}}
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
            for a in self.cluster.get_aliases()
            if re.match(re.escape(alias) + "\\d{8}_\\d{6}", a.index) and not a.alias
        ])
        return output

    def get_index(self, alias):
        """
        RETURN THE INDEX USED BY THIS alias
        """
        output = sort([
            a.index
            for a in self.cluster.get_aliases()
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
        for a in self.cluster.get_aliases():
            if a.index == index and a.alias:
                return False
        return True

    def delete_record(self, filter):
        self.cluster.get_metadata()
        if self.cluster.node_metatdata.version.number.startswith("0.90"):
            query = filter
        elif self.cluster.node_metatdata.version.number.startswith("1.0"):
            query = {"query": filter}
        else:
            raise NotImplementedError

        if self.debug:
            Log.note("Delete bugs:\n{{query}}", {"query": query})

        self.cluster.delete(
            self.path + "/_query",
            data=CNV.object2JSON(query),
            timeout=60
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
                if id == None:
                    id = Random.hex(40)

                if "json" in r:
                    json = r["json"]
                elif "value" in r:
                    json = CNV.object2JSON(r["value"])
                else:
                    json = None
                    Log.error("Expecting every record given to have \"value\" or \"json\" property")

                lines.append('{"index":{"_id": ' + CNV.object2JSON(id) + '}}')
                lines.append(json)
            del records

            if not lines:
                return

            try:
                data_bytes = "\n".join(lines) + "\n"
                data_bytes = data_bytes.encode("utf8")
                del lines
            except Exception, e:
                Log.error("can not make request body from\n{{lines|indent}}", {"lines": lines}, e)

            response = self.cluster._post(
                self.path + "/_bulk",
                data=data_bytes,
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
                Log.note("{{num}} items added", {"num": len(items)})
        except Exception, e:
            if e.message.startswith("sequence item "):
                Log.error("problem with {{data}}", {"data": repr(lines[int(e.message[14:16].strip())])}, e)
            Log.error("problem sending to ES", e)


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

        response = self.cluster.put(
            "/" + self.settings.index + "/_settings",
            data="{\"index.refresh_interval\":\"" + interval + "\"}"
        )

        result = CNV.JSON2object(utf82unicode(response.content))
        if not result.ok:
            Log.error("Can not set refresh interval ({{error}})", {
                "error": utf82unicode(response.content)
            })

    def search(self, query, timeout=None):
        query = wrap(query)
        try:
            if self.debug:
                if len(query.facets.keys()) > 20:
                    show_query = query.copy()
                    show_query.facets = {k: "..." for k in query.facets.keys()}
                else:
                    show_query = query
                Log.note("Query:\n{{query|indent}}", {"query": show_query})
            return self.cluster._post(
                self.path + "/_search",
                data=CNV.object2JSON(query).encode("utf8"),
                timeout=nvl(timeout, self.settings.timeout)
            )
        except Exception, e:
            Log.error("Problem with search (path={{path}}):\n{{query|indent}}", {
                "path": self.path + "/_search",
                "query": query
            }, e)

    def threaded_queue(self, size=None, period=None):
        return ThreadedQueue(self, size=size, period=period)

    def delete(self):
        self.cluster.delete_index(index=self.settings.index)


class Cluster(object):
    def __init__(self, settings):
        """
        settings.explore_metadata == True - IF PROBING THE CLUSTER FOR METATDATA IS ALLOWED
        settings.timeout == NUMBER OF SECONDS TO WAIT FOR RESPONSE, OR SECONDS TO WAIT FOR DOWNLOAD (PASSED TO requests)
        """

        settings = wrap(settings)
        assert settings.host
        settings.setdefault("explore_metadata", True)

        self.cluster_metadata = None
        settings.setdefault("port", 9200)
        self.debug = nvl(settings.debug, DEBUG)
        self.settings = settings
        self.path = settings.host + ":" + unicode(settings.port)

    def get_or_create_index(self, settings, schema=None, limit_replicas=None):
        settings = deepcopy(settings)
        aliases = self.get_aliases()
        indexes = [a for a in aliases if a.alias == settings.index or a.index == settings.index]
        if not indexes:
            self.create_index(settings, schema, limit_replicas=limit_replicas)
        elif len(indexes) > 1:
            Log.error("More than one match")
        elif indexes[0].alias != None:
            settings.alias = indexes[0].alias
            settings.index = indexes[0].index
        return Index(settings)

    def get_index(self, settings):
        """
        TESTS THAT THE INDEX EXISTS BEFORE RETURNING A HANDLE
        """
        aliases = self.get_aliases()
        if settings.index in aliases.index:
            return Index(settings)
        if settings.index in aliases.alias:
            match = [a for a in aliases if a.alias == settings.index][0]
            settings.alias = match.alias
            settings.index = match.index
            return Index(settings)
        Log.error("Can not find index {{index_name}}", {"index_name": settings.index})

    def create_index(self, settings, schema=None, limit_replicas=None):
        if not settings.alias:
            settings.alias = settings.index
            settings.index = proto_name(settings.alias)

        if settings.alias == settings.index:
            Log.error("Expecting index name to conform to pattern")

        if not schema and settings.schema_file:
            from .files import File

            schema = CNV.JSON2object(File(settings.schema_file).read(), flexible=True, paths=True)
        elif isinstance(schema, basestring):
            schema = CNV.JSON2object(schema, paths=True)
        else:
            schema = CNV.JSON2object(CNV.object2JSON(schema), paths=True)

        limit_replicas = nvl(limit_replicas, settings.limit_replicas)

        if limit_replicas:
            # DO NOT ASK FOR TOO MANY REPLICAS
            health = self.get("/_cluster/health")
            if schema.settings.index.number_of_replicas >= health.number_of_nodes:
                Log.warning("Reduced number of replicas: {{from}} requested, {{to}} realized", {
                    "from": schema.settings.index.number_of_replicas,
                    "to": health.number_of_nodes - 1
                })
                schema.settings.index.number_of_replicas = health.number_of_nodes - 1

        self._post(
            "/" + settings.index,
            data=CNV.object2JSON(schema).encode("utf8"),
            headers={"Content-Type": "application/json"}
        )
        time.sleep(2)
        es = Index(settings)
        return es

    def delete_index(self, index=None):
        self.delete("/" + index)

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
        if self.settings.explore_metadata:
            if not self.cluster_metadata:
                response = self.get("/_cluster/state")
                self.cluster_metadata = response.metadata
                self.node_metatdata = self.get("/")
        else:
            Log.error("Metadata exploration has been disabled")
        return self.cluster_metadata

    def _post(self, path, *args, **kwargs):
        if "data" in kwargs and not isinstance(kwargs["data"], str):
            Log.error("data must be utf8 encoded string")

        url = self.settings.host + ":" + unicode(self.settings.port) + path

        try:
            kwargs = wrap(kwargs)
            kwargs.setdefault("timeout", 600)
            kwargs.headers["Accept-Encoding"] = "gzip,deflate"
            kwargs = unwrap(kwargs)
            response = requests.post(url, *args, **kwargs)
            if self.debug:
                Log.note(utf82unicode(response.content)[:130])
            details = CNV.JSON2object(utf82unicode(response.content))
            if details.error:
                Log.error(CNV.quote2string(details.error))
            if details._shards.failed > 0:
                Log.error("Shard failure")
            return details
        except Exception, e:
            if url[0:4] != "http":
                suggestion = " (did you forget \"http://\" prefix on the host name?)"
            else:
                suggestion = ""

            Log.error("Problem with call to {{url}}" + suggestion + "\n{{body}}", {
                "url": url,
                "body": kwargs["data"] if DEBUG else kwargs["data"][0:100]
            }, e)

    def get(self, path, **kwargs):
        url = self.settings.host + ":" + unicode(self.settings.port) + path
        try:
            kwargs = wrap(kwargs)
            kwargs.setdefault("timeout", 600)
            response = requests.get(url, **kwargs)
            if self.debug:
                Log.note(utf82unicode(response.content)[:130])
            details = wrap(CNV.JSON2object(utf82unicode(response.content)))
            if details.error:
                Log.error(details.error)
            return details
        except Exception, e:
            Log.error("Problem with call to {{url}}", {"url": url}, e)

    def put(self, path, *args, **kwargs):
        url = self.settings.host + ":" + unicode(self.settings.port) + path
        try:
            kwargs = wrap(kwargs)
            kwargs.setdefault("timeout", 60)
            response = requests.put(url, *args, **kwargs)
            if self.debug:
                Log.note(utf82unicode(response.content))
            return response
        except Exception, e:
            Log.error("Problem with call to {{url}}", {"url": url}, e)

    def delete(self, path, *args, **kwargs):
        url = self.settings.host + ":" + unicode(self.settings.port) + path
        try:
            kwargs.setdefault("timeout", 60)
            response = requests.delete(url, **kwargs)
            if self.debug:
                Log.note(utf82unicode(response.content))
            return response
        except Exception, e:
            Log.error("Problem with call to {{url}}", {"url": url}, e)


def proto_name(prefix, timestamp=None):
    if not timestamp:
        timestamp = datetime.utcnow()
    return prefix + CNV.datetime2string(timestamp, "%Y%m%d_%H%M%S")


def sort(values):
    return wrap(sorted(values))


def scrub(r):
    """
    REMOVE KEYS OF DEGENERATE VALUES (EMPTY STRINGS, EMPTY LISTS, AND NULLS)
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


