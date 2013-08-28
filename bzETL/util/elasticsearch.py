import sha
import requests
import time
from util.cnv import CNV
from util.debug import D
from util.basic import nvl
from util.struct import Struct, StructList

DEBUG=True

class ElasticSearch():




    def __init__(self, settings):
        assert settings.host is not None
        assert settings.index is not None
        assert settings.type is not None

        self.metadata=None
        if settings.port is None: settings.port=9200
        
        self.settings=settings
        self.path=settings.host+":"+str(settings.port)+"/"+settings.index+"/"+settings.type



    @staticmethod
    def create_index(settings, schema):
        if isinstance(schema, basestring):
            schema=CNV.JSON2object(schema)

        ElasticSearch.post(
            settings.host+":"+str(settings.port)+"/"+settings.index,
            data=CNV.object2JSON(schema),
            headers={"Content-Type":"application/json"}
        )
        time.sleep(2)
        es=ElasticSearch(settings)
        es.add_alias(settings.alias)
        return es




    @staticmethod
    def delete_index(settings, index=None):
        index=nvl(index, settings.index)

        ElasticSearch.delete(
            settings.host+":"+str(settings.port)+"/"+index,
        )

    #RETURN LIST OF {"alias":a, "index":i} PAIRS
    #ALL INDEXES INCLUDED, EVEN IF NO ALIAS {"alias":None}
    def get_aliases(self):
        data=self.get_metadata().indices
        output=[]
        for index, desc in data.items():
            if desc["aliases"] is None or len(desc["aliases"])==0:
                output.append({"index":index, "alias":None})
            else:
                for a in desc["aliases"]:
                    output.append({"index":index, "alias":a})
        return StructList(output)


    
    def get_metadata(self):
        if self.metadata is None:
            response=self.get(self.settings.host+":"+str(self.settings.port)+"/_cluster/state")
            self.metadata=response.metadata
        return self.metadata


    def get_schema(self):
        return self.get_metadata().indicies[self.settings.index]


    #DELETE ALL INDEXES WITH GIVEN PREFIX, EXCEPT name
    def delete_all_but(self, prefix, name):
        for a in self.get_aliases():
            if a.index.startswith(prefix) and a.index!=name:
                ElasticSearch.delete_index(self.settings, a.index)


    def add_alias(self, alias):
        requests.post(
            self.settings.host+":"+str(self.settings.port)+"/_aliases",
            CNV.object2JSON({
                "actions":[
                    {"add":{"index":self.settings.index, "alias":alias}}
                ]
            })
        )

    # RECORDS MUST HAVE id AND json AS A STRING OR
    # HAVE id AND value AS AN OBJECT
    def add(self, records):
        # ADD LINE WITH COMMAND
        lines=[]
        for r in records:
            id=r["id"]
            if "json" in r:
                json=r["json"]
            else:
                json=CNV.object2JSON(r["value"])
                
            if id is None: id=sha.new(json).hexdigest()

            lines.append('{"index":{"_id":"'+id+'"}}')
            lines.append(json)

        if len(lines)==0: return
        response=ElasticSearch.post(
            self.path+"/_bulk",
            data="\n".join(lines)+"\n",
            headers={"Content-Type":"text"}
        )
        items=response["items"]

        for i, item in enumerate(items):
            if not item.index.ok:
                D.error("{{error}} while loading line:\n{{line}}", {
                    "error":item.index.error,
                    "line":lines[i*2+1]
                })

        if DEBUG: D.println("{{num}} items added", {"num":len(lines)/2})


    # -1 FOR NO REFRESH
    def set_refresh_interval(self, seconds):
        if seconds<=0: interval="-1"
        else: interval=str(seconds)+"s"

        ElasticSearch.put(
             self.settings.host+":"+str(self.settings.port)+"/"+self.settings.index+"/_settings",
             data="{\"index.refresh_interval\":\""+interval+"\"}"
        )



    def search(self, query):
        try:
            return ElasticSearch.post(self.path+"/_search", data=CNV.object2JSON(query))
        except Exception, e:
            D.error("Problem with search", e)

    
        
    @staticmethod
    def post(*list, **args):
        try:
            response=requests.post(*list, **args)
            if DEBUG: D.println(response.content[:130])
            details=CNV.JSON2object(response.content)
            if details.error is not None:
                D.error(details.error)
            return details
        except Exception, e:
            D.error("Problem with call to {{url}}", {"url":list[0]}, e)

    @staticmethod
    def get(*list, **args):
        try:
            response=requests.get(*list, **args)
            if DEBUG: D.println(response.content[:130])
            details=CNV.JSON2object(response.content)
            if details.error is not None:
                D.error(details.error)
            return details
        except Exception, e:
            D.error("Problem with call to {{url}}", {"url":list[0]}, e)

    @staticmethod
    def put(*list, **args):
        try:
            response=requests.put(*list, **args)
            if DEBUG: D.println(response.content)
            return response
        except Exception, e:
            D.error("Problem with call to {{url}}", {"url":list[0]}, e)

    @staticmethod
    def delete(*list, **args):
        try:
            response=requests.delete(*list, **args)
            if DEBUG: D.println(response.content)
            return response
        except Exception, e:
            D.error("Problem with call to {{url}}", {"url":list[0]}, e)


