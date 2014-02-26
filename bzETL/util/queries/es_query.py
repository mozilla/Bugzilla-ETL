# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals

from ..cnv import CNV
from ..env.elasticsearch import ElasticSearch
from ..queries import MVEL
from ..queries.es_query_aggop import is_aggop, es_aggop
from ..queries.es_query_setop import is_fieldop, is_setop, is_deep, es_setop, es_deepop, es_fieldop
from ..queries.es_query_terms import es_terms, is_terms
from ..queries.es_query_terms_stats import es_terms_stats, is_terms_stats
from ..queries.es_query_util import aggregates, loadColumns
from . import Q
from ..queries.dimensions import Dimension
from ..queries.query import Query, _normalize_where
from ..env.logs import Log
from ..queries.MVEL import _MVEL
from ..struct import Struct, split_field, wrap, listwrap


class ESQuery(object):
    """
    SEND GENERAL Qb QUERIES TO ElasticSearch
    """
    def __init__(self, es):
        self.es = es
        self.edges = Struct()

    def query(self, query):
        query = Query(query, schema=self)

        for s in listwrap(query.select):
            if not aggregates[s.aggregate]:
                Log.error("ES can not aggregate " + self.select[0].name + " because '" + self.select[0].aggregate + "' is not a recognized aggregate")

        frum = query["from"]
        if isinstance(frum, Query):
            result = self.query(frum)
            q2 = query.copy()
            q2.frum = result
            return Q.run(q2)

        frum = loadColumns(self.es, query["from"])
        mvel = _MVEL(frum)

        if is_fieldop(query):
            return es_fieldop(self.es, query)
        elif is_deep(query):
            return es_deepop(self.es, mvel, query)
        elif is_setop(query):
            return es_setop(self.es, mvel, query)
        elif is_aggop(query):
            return es_aggop(self.es, mvel, query)
        elif is_terms(query):
            return es_terms(self.es, mvel, query)
        elif is_terms_stats(query):
            return es_terms_stats(self.es, mvel, query)

        Log.error("Can not handle")


    def addDimension(self, dim):
        self._addDimension(dim, [])

    def _addDimension(self, dim, path):
        dim.full_name = dim.name
        for e in dim.edges:
            d = Dimension(e, dim, self)
            self.edges[d.full_name] = d

    def __getitem__(self, item):
        f = split_field(item)
        e = self.edges[f[0]]
        for i in f[1::]:
            e = e[i]
        return e

    def __getattr__(self, item):
        return self.edges[item]


    def update(self, command):
        """
        EXPECTING command == {"set":term, "where":where}
        THE set CLAUSE IS A DICT MAPPING NAMES TO VALUES
        THE where CLAUSE IS AN ES FILTER
        """
        command = wrap(command)

        #GET IDS OF DOCUMENTS
        results = self.es.search({
            "fields": [],
            "query": {"filtered": {
                "query": {"match_all": {}},
                "filter": _normalize_where(command.where, self)
            }},
            "size": 200000
        })

        scripts = []
        for k, v in command.set.items():
            if not MVEL.isKeyword(k):
                Log.error("Only support simple paths for now")

            scripts.append("ctx._source."+k+" = "+MVEL.value2MVEL(v)+";")
        script = "".join(scripts)

        for id in results.hits.hits._id:
            #SEND UPDATE TO EACH
            try:
                response = ElasticSearch.post(
                    self.es.path + "/" + id + "/_update",
                    data=CNV.object2JSON({"script": script}).encode("utf8"),
                    headers={"Content-Type": "application/json"}
                )

                if not response.ok:
                    Log.error("Problem updating es: {{error}}", {"error":response})
            except Exception, e:
                Log.error("Problem updating es: {{error}}", e)

