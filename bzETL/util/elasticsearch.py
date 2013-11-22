# encoding: utf-8
#
from datetime import datetime
import re
import sha
import time

import requests
from .threads import ThreadedQueue

import struct
from .maths import Math
from .queries import Q
from .cnv import CNV
from .logs import Log
from .struct import nvl
from .struct import Struct, StructList

DEBUG=False


class ElasticSearch(object):




    def __init__(self, settings):
        assert settings.host
        assert settings.index
        assert settings.type

        self.metadata = None
        if not settings.port: settings.port=9200
        self.debug=nvl(settings.debug, DEBUG)
        globals()["DEBUG"]=DEBUG or self.debug

        self.settings=settings
        self.path=settings.host+":"+unicode(settings.port)+"/"+settings.index+"/"+settings.type



    @staticmethod
    def create_index(settings, schema):
        if isinstance(schema, basestring):
            schema=CNV.JSON2object(schema)

        ElasticSearch.post(
            settings.host+":"+unicode(settings.port)+"/"+settings.index,
            data=CNV.object2JSON(schema),
            headers={"Content-Type":"application/json"}
        )
        time.sleep(2)
        es=ElasticSearch(settings)
        return es




    @staticmethod
    def delete_index(settings, index=None):
        index=nvl(index, settings.index)

        ElasticSearch.delete(
            settings.host+":"+unicode(settings.port)+"/"+index,
        )

    def get_aliases(self):
        """
        RETURN LIST OF {"alias":a, "index":i} PAIRS
        ALL INDEXES INCLUDED, EVEN IF NO ALIAS {"alias":Null}
        """
        data=self.get_metadata().indices
        output=[]
        for index, desc in data.items():
            if not desc["aliases"]:
                output.append({"index":index, "alias":None})
            else:
                for a in desc["aliases"]:
                    output.append({"index":index, "alias":a})
        return struct.wrap(output)



    def get_metadata(self):
        if not self.metadata:
            response=self.get(self.settings.host+":"+unicode(self.settings.port)+"/_cluster/state")
            self.metadata=response.metadata
        return self.metadata


    def get_schema(self):
        return self.get_metadata().indicies[self.settings.index]



    #DELETE ALL INDEXES WITH GIVEN PREFIX, EXCEPT name
    def delete_all_but(self, prefix, name):
        for a in self.get_aliases():
            # MATCH <prefix>YYMMDD_HHMMSS FORMAT
            if re.match(re.escape(prefix)+"\\d{8}_\\d{6}", a.index) and a.index!=name:
                ElasticSearch.delete_index(self.settings, a.index)


    @staticmethod
    def proto_name(prefix, timestamp=None):
        if not timestamp:
            timestamp = datetime.utcnow()
        return prefix + CNV.datetime2string(timestamp, "%Y%m%d_%H%M%S")


    def add_alias(self, alias):
        self.metadata = None
        requests.post(
            self.settings.host+":"+unicode(self.settings.port)+"/_aliases",
            CNV.object2JSON({
                "actions":[
                    {"add":{"index":self.settings.index, "alias":alias}}
                ]
            })
        )

    def get_proto(self, alias):
        """
        RETURN ALL INDEXES THAT ARE INTENDED TO BE GIVEN alias, BUT HAVE NO
        ALIAS YET BECAUSE INCOMPLETE
        """
        output=Q.sort([
            a.index
            for a in self.get_aliases()
            if re.match(re.escape(alias)+"\\d{8}_\\d{6}", a.index) and not a.alias
        ])
        return output

    def is_proto(self, index):
        """
        RETURN True IF THIS INDEX HAS NOT BEEN ASSIGNED IT'S ALIAS
        """
        for a in self.get_aliases():
            if a.index==index and a.alias:
                return False
        return True


    def delete_record(self, query):
        if isinstance(query, dict):
            ElasticSearch.delete(
                self.path+"/_query",
                data=CNV.object2JSON(query)
            )
        else:
            ElasticSearch.delete(
                self.path+"/"+query
            )

    def extend(self, records):
        # ADD LINE WITH COMMAND
        lines=[]
        for r in records:
            id=r["id"]
            if "json" in r:
                json=r["json"]
            elif "value" in r:
                json=CNV.object2JSON(r["value"])
            else:
                Log.error("Expecting every record given to have \"value\" or \"json\" property")

            if id == None:
                id = sha.new(json).hexdigest()

            lines.append('{"index":{"_id":'+CNV.object2JSON(id)+'}}')
            lines.append(json)

        if not lines: return
        response=ElasticSearch.post(
            self.path+"/_bulk",
            data="\n".join(lines).encode("utf8")+"\n",
            headers={"Content-Type":"text"}
        )
        items=response["items"]

        for i, item in enumerate(items):
            if not item.index.ok:
                Log.error("{{error}} while loading line:\n{{line}}", {
                    "error":item.index.error,
                    "line":lines[i*2+1]
                })

        if self.debug:
            Log.note("{{num}} items added", {"num":len(lines)/2})


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

        response=ElasticSearch.put(
            self.settings.host + ":" + unicode(
                self.settings.port) + "/" + self.settings.index + "/_settings",
            data="{\"index.refresh_interval\":\"" + interval + "\"}"
        )

        if response.content != '{"ok":true}':
            Log.error("Can not set refresh interval ({{error}})", {
                "error": response.content
            })


    def search(self, query):
        try:
            return ElasticSearch.post(self.path+"/_search", data=CNV.object2JSON(query))
        except Exception, e:
            Log.error("Problem with search", e)

    def threaded_queue(self, size):
        return ThreadedQueue(self, size)

    @staticmethod
    def post(*list, **args):
        try:
            response=requests.post(*list, **args)
            if DEBUG: Log.note(response.content[:130])
            details=CNV.JSON2object(response.content)
            if details.error:
                Log.error(details.error)
            return details
        except Exception, e:
            Log.error("Problem with call to {{url}}", {"url":list[0]}, e)

    @staticmethod
    def get(*list, **args):
        try:
            response=requests.get(*list, **args)
            if DEBUG: Log.note(response.content[:130])
            details=CNV.JSON2object(response.content)
            if details.error:
                Log.error(details.error)
            return details
        except Exception, e:
            Log.error("Problem with call to {{url}}", {"url":list[0]}, e)

    @staticmethod
    def put(*list, **args):
        try:
            response=requests.put(*list, **args)
            if DEBUG: Log.note(response.content)
            return response
        except Exception, e:
            Log.error("Problem with call to {{url}}", {"url":list[0]}, e)

    @staticmethod
    def delete(*list, **args):
        try:
            response=requests.delete(*list, **args)
            if DEBUG: Log.note(response.content)
            return response
        except Exception, e:
            Log.error("Problem with call to {{url}}", {"url":list[0]}, e)


    @staticmethod
    def scrub(r):
        """
        REMOVE KEYS OF DEGENERATE VALUES (EMPTY STRINGS, EMPTY LISTS, AND NULLS)
        TO LOWER CASE
        CONVERT STRINGS OF NUMBERS TO NUMBERS
        RETURNS **COPY**, DOES NOT CHANGE ORIGINAL
        """
        return struct.wrap(_scrub(r))


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
                r = r.dict
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
                return Q.sort(output)
            except Exception:
                return output
        else:
            return r
    except Exception, e:
        Log.warning("Can not scrub: {{json}}", {"json": r})




