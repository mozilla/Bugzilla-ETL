# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#





from bzETL.extract_bugzilla import get_all_cc_changes
from pyLibrary import convert
from pyLibrary.collections import Multiset
from pyLibrary.debugs import startup
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import set_default, coalesce
from pyLibrary.env import elasticsearch
from pyLibrary.queries import jx
from pyLibrary.queries.jx_usingES import FromES
from pyLibrary.sql.mysql import MySQL


def full_analysis(settings, bug_list=None, please_stop=None):
    """
    THE CC LISTS (AND REVIEWS) ARE EMAIL ADDRESSES THE BELONG TO PEOPLE.
    SINCE THE EMAIL ADDRESS FOR A PERSON CAN CHANGE OVER TIME.  THIS CODE
    WILL ASSOCIATE EACH PERSON WITH THE EMAIL ADDRESSES USED
    OVER THE LIFETIME OF THE BUGZILLA DATA.  'PERSON' IS ABSTRACT, AND SIMPLY
    ASSIGNED A CANONICAL EMAIL ADDRESS TO FACILITATE IDENTIFICATION
    """
    if settings.args.quick:
        Log.note("Alias analysis skipped (--quick was used)")
        return

    analyzer = AliasAnalyzer(settings.alias)

    if bug_list:
        with MySQL(settings.bugzilla, readonly=True) as db:
            data = get_all_cc_changes(db, bug_list)
            analyzer.aggregator(data)
            analyzer.analysis(True, please_stop)
        return

    with MySQL(settings=settings.bugzilla, readonly=True) as db:
        start = coalesce(settings.alias.start, 0)
        end = coalesce(settings.alias.end, db.query("SELECT max(bug_id)+1 bug_id FROM bugs")[0].bug_id)

        #Perform analysis on blocks of bugs, in case we crash partway through
        for s, e in jx.intervals(start, end, settings.alias.increment):
            Log.note(
                "Load range {{start}}-{{end}}",
                start=s,
                end=e
            )
            data = get_all_cc_changes(db, list(range(s, e)))
            if please_stop:
                break
            analyzer.aggregator(data)
            analyzer.analysis(e >= end, please_stop)


class AliasAnalyzer(object):

    def __init__(self, settings):
        self.bugs={}
        self.aliases={}
        self.not_aliases={}  # EXPLICIT LIST OF NON-MATCHES (HUMAN ADDED)
        try:
            a = set_default({}, settings.elasticsearch, {"type":"alias"})
            self.es = elasticsearch.Cluster(settings.elasticsearch).get_or_create_index(settings=a, schema=ALIAS_SCHEMA, limit_replicas=True)
            self.esq = FromES(self.es.settings)
            result = self.esq.query({
                "from":"bug_aliases",
                "select":["canonical", "alias"]
            })
            for r in result:
                self.aliases[r.alias] = {"canonical":r["canonical"], "dirty":False}

            Log.note("{{num}} aliases loaded", num=len(list(self.aliases.keys())))

            # LOAD THE NON-MATCHES
            na = set_default({}, settings.elasticsearch, {"type":"not_alias"})
            es = elasticsearch.Cluster(na).get_or_create_index(na)
            esq = FromES(es.settings)
            result = esq.query({
                "from":"bug_aliases",
                "select":["canonical", "alias"]
            })
            for r in result:
                self.not_aliases[r.alias] = r["canonical"]

        except Exception as e:
            Log.error("Can not init aliases", cause=e)

    def aggregator(self, data):
        """
        FLATTEN CC LISTS OVER TIME BY BUG
        MULTISET COUNTS THE NUMBER OF EMAIL AT BUG CREATION
        NEGATIVE MEANS THERE WAS AN ADD WITHOUT A REMOVE (AND NOT IN CURRENT LIST)
        """
        for d in data:
            new_emails = mapper(split_email(d.new_value), self.aliases)
            old_emails = mapper(split_email(d.old_value), self.aliases)

            agg = self.bugs.get(d.bug_id, Multiset(allow_negative=True))
            agg = agg - new_emails
            agg = agg + old_emails
            self.bugs[d.bug_id] = agg

    def analysis(self, last_run, please_stop):
        DIFF = 7
        if last_run:
            DIFF = 4      #ONCE WE HAVE ALL THE DATA IN WE CAN BE LESS DISCRIMINATING
        try_again = True

        while try_again and not please_stop:
            #FIND EMAIL MOST NEEDING REPLACEMENT
            problem_agg = Multiset(allow_negative=True)
            for bug_id, agg in self.bugs.items():
                #ONLY COUNT NEGATIVE EMAILS
                for email, count in agg.dic.items():
                    if count < 0:
                        problem_agg.add(self.alias(email)["canonical"], amount=count)

            problems = jx.sort([
                {"email": e, "count": c}
                for e, c in problem_agg.dic.items()
                if not self.not_aliases.get(e, None) and (c <= -(DIFF / 2) or last_run)
            ], ["count", "email"])

            try_again = False
            for problem in problems:
                if please_stop:
                    break

                #FIND MOST LIKELY MATCH
                solution_agg = Multiset(allow_negative=True)
                for bug_id, agg in self.bugs.items():
                    if agg.dic.get(problem.email, 0) < 0:  #ONLY BUGS THAT ARE EXPERIENCING THIS problem
                        solution_agg += agg
                solutions = jx.sort([{"email": e, "count": c} for e, c in solution_agg.dic.items()], [{"field": "count", "sort": -1}, "email"])

                if last_run and len(solutions) == 2 and solutions[0].count == -solutions[1].count:
                    #exact match
                    pass
                elif len(solutions) <= 1 or (solutions[1].count + DIFF >= solutions[0].count):
                    #not distinctive enough
                    continue

                best_solution = solutions[0]
                Log.note(
                    "{{problem}} ({{score}}) -> {{solution}} {{matches}}",
                    problem= problem.email,
                    score= problem.count,
                    solution= best_solution.email,
                    matches= convert.value2json(jx.select(solutions, "count")[:10:])
                )
                try_again = True
                self.add_alias(problem.email, best_solution.email)

        self.saveAliases()


    def alias(self, email):
        canonical = self.aliases.get(email, None)
        if not canonical:
            canonical = self.esq.query({
                "from":"bug_aliases",
                "select":"canonical",
                "where":{"term":{"alias":email}}
            })
            if not canonical:
                canonical = {"canonical":email, "dirty":False}
            else:
                canonical = {"canonical":canonical[0], "dirty":False}

            self.aliases[email] = canonical

        return canonical


    def add_alias(self, lost, found):
        new_canonical = self.alias(found)
        old_canonical = self.alias(lost)

        delete_list = []

        #FOLD bugs ON lost=found
        for bug_id, agg in self.bugs.items():
            v = agg.dic.get(lost, 0)
            if v != 0:
                agg.add(lost, -v)
                agg.add(found, v)

            if not agg:
                delete_list.append(bug_id)

        #FOLD bugs ON old_canonical=new_canonical
        if old_canonical["canonical"] != lost:
            for bug_id, agg in self.bugs.items():
                v = agg.dic.get(old_canonical["canonical"], 0)
                if v != 0:
                    agg.add(old_canonical["canonical"], -v)
                    agg.add(new_canonical["canonical"], v)

                if not agg:
                    delete_list.append(bug_id)

        for d in delete_list:
            del self.bugs[d]

        #FOLD ALIASES
        reassign=[]
        for k, v in self.aliases.items():
            if v["canonical"] == old_canonical["canonical"]:
                Log.note(
                    "ALIAS REMAPPED: {{alias}}->{{old}} to {{alias}}->{{new}}",
                    alias= k,
                    old= old_canonical["canonical"],
                    new= found
                )
                reassign.append((k, found))

        for k, found in reassign:
            self.aliases[k] = {"canonical":found, "dirty":True}

    def saveAliases(self):
        records = []
        for k, v in self.aliases.items():
            if v["dirty"]:
                records.append({"id":k, "value":{"canonical":v["canonical"], "alias":k}})

        if records:
            Log.note("Net new aliases saved: {{num}}", num=len(records))
            self.es.extend(records)

def mapper(emails, aliases):
    output = set()
    for e in emails:
        canonical = aliases.get(e, None)
        if not canonical:
            canonical = {"canonical": e, "dirty": False}
            aliases[e] = canonical
        output.add(canonical["canonical"])
    return output

def split_email(value):
    if not value:
        return set()

    if value.startswith("?") or value.endswith("?"):
        return set()
    return set([s.strip() for s in value.split(",") if s.strip() != ""])


def start():
    try:
        settings = startup.read_settings()
        Log.start(settings.debug)
        full_analysis(settings)
    except Exception as e:
        Log.error("Can not start", e)
    finally:
        Log.stop()

ALIAS_SCHEMA = {
    "settings": {"index": {
        "number_of_shards": 3,
        "number_of_replicas": 0,
        "store": {"throttle": {
            "type": "merge",
            "max_bytes_per_sec": "2mb"
        }},
        "cache": {
            "expire": "1m",
            "field.type": "soft"
        }
    }},
    "mappings": {
        "alias": {
            "_all": {
                "enabled": False
            },
            "_source": {
                "compress": False,
                "enabled": True
            },
            "_id":{
                "path": "canonical"
            },
            "properties": {
                "canonical": {
                    "type": "string",
                    "index": "not_analyzed",
                    "store": "yes"
                },
                "alias": {
                    "type": "string",
                    "index": "not_analyzed",
                    "store": "yes"
                }
            }
        },
        "not_alias": {
            "_all": {
                "enabled": False
            },
            "_source": {
                "compress": False,
                "enabled": True
            },
            "properties": {
                "a": {
                    "type": "string",
                    "index": "not_analyzed",
                    "store": "yes"
                },
                "b": {
                    "type": "string",
                    "index": "not_analyzed",
                    "store": "yes"
                }
            }
        }
    }
}


if __name__ == "__main__":
    start()


