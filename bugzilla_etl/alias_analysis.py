# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import os

import jx_elasticsearch
from bugzilla_etl.extract_bugzilla import get_all_cc_changes
from jx_python import jx
from mo_collections.multiset import Multiset
from mo_dots import coalesce
from mo_files import File
from mo_future import iteritems
from mo_json import value2json, json2value
from mo_kwargs import override
from mo_logs import Log, startup, constants
from mo_math.randoms import Random
from mo_testing.fuzzytestcase import assertAlmostEqual
from pyLibrary.convert import zip2bytes, bytes2zip
from pyLibrary.env.elasticsearch import Cluster
from pyLibrary.sql.mysql import MySQL

DEBUG = True
MINIMUM_DIFF_ROUGH = 7
MINIMUM_DIFF_FINE = 4


def full_analysis(kwargs, bug_list=None, please_stop=None):
    """
    THE CC LISTS (AND REVIEWS) ARE EMAIL ADDRESSES THE BELONG TO PEOPLE.
    SINCE THE EMAIL ADDRESS FOR A PERSON CAN CHANGE OVER TIME.  THIS CODE
    WILL ASSOCIATE EACH PERSON WITH THE EMAIL ADDRESSES USED
    OVER THE LIFETIME OF THE BUGZILLA DATA.  'PERSON' IS ABSTRACT, AND SIMPLY
    ASSIGNED A CANONICAL EMAIL ADDRESS TO FACILITATE IDENTIFICATION
    """
    if kwargs.args.quick:
        Log.note("Alias analysis skipped (--quick was used)")
        return

    analyzer = AliasAnalyzer(kwargs.alias)

    if bug_list:
        with MySQL(kwargs=kwargs.bugzilla, readonly=True) as db:
            data = get_all_cc_changes(db, bug_list)
            analyzer.aggregator(data)
            analyzer.analysis(True, please_stop)
        return

    with MySQL(kwargs=kwargs.bugzilla, readonly=True) as db:
        start = coalesce(kwargs.alias.start, 0)
        end = coalesce(kwargs.alias.end, db.query("SELECT max(bug_id)+1 bug_id FROM bugs")[0].bug_id)

        #Perform analysis on blocks of bugs, in case we crash partway through
        for s, e in Random.combination(jx.intervals(start, end, kwargs.alias.increment)):
            while not please_stop:
                try:
                    with db.transaction():
                        Log.note("Load range {{start}}-{{end}}", start=s, end=e)
                        if please_stop:
                            break
                        data = get_all_cc_changes(db, range(s, e))
                        analyzer.aggregator(data)
                    analyzer.analysis(last_run=False, please_stop=please_stop)
                    break
                except Exception as f:
                    Log.warning("failure while performing analysis", cause=f)


class AliasAnalyzer(object):

    @override
    def __init__(
        self,
        elasticsearch=None, # ES INDEX TO STORE THE ALIASES
        file=None,         # FILE TO STORE ALIASES (IF ES DOES NOT EXIST, OR IS EMPTY)
        start=0,           # MINIMUM BUG NUMBER TO SCAN
        increment=100000,  # NUMBER OF BUGS TO REVIEW IN ONE PASS
        minimum_diff=MINIMUM_DIFF_ROUGH,  # AMOUNT OF DISPARITY BETWEEN BEST AND SECOND-BEST MATCH
        kwargs=None
    ):
        self.bugs = {}
        self.aliases = {}
        self.not_aliases = {}  # EXPLICIT LIST OF NON-MATCHES (HUMAN ADDED)
        self.kwargs = kwargs
        self.es = None

        self.load_aliases()


    def aggregator(self, data):
        """
        FLATTEN CC LISTS OVER TIME BY BUG
        MULTISET COUNTS THE NUMBER OF EMAIL AT BUG CREATION
        NEGATIVE MEANS THERE WAS AN ADD WITHOUT A REMOVE (AND NOT IN CURRENT LIST)
        """
        for d in data:
            new_emails = mapper(split_email(d.new_value), self.aliases)
            old_emails = mapper(split_email(d.old_value), self.aliases)

            agg = self.bugs.get(d.bug_id) or Multiset(allow_negative=True)
            agg = agg - new_emails
            agg = agg + old_emails
            self.bugs[d.bug_id] = agg

    def analysis(self, last_run, please_stop):
        minimum_diff = self.kwargs.minimum_diff
        if last_run:
            minimum_diff = min(minimum_diff, MINIMUM_DIFF_FINE)     #ONCE WE HAVE ALL THE DATA IN WE CAN BE LESS DISCRIMINATING
        try_again = True

        Log.note("running analysis with minimum_diff=={{minimum_diff}}", minimum_diff=minimum_diff)

        while try_again and not please_stop:
            # FIND EMAIL MOST NEEDING REPLACEMENT
            problem_agg = Multiset(allow_negative=True)
            for bug_id, agg in iteritems(self.bugs):
                #ONLY COUNT NEGATIVE EMAILS
                for email, count in iteritems(agg.dic):
                    if count < 0:
                        problem_agg.add(self.get_canonical(email), amount=count)

            problems = jx.sort(
                [
                    {"email": e, "count": c}
                    for e, c in iteritems(problem_agg.dic)
                    if not self.not_aliases.get(e, None) and (c <= -(minimum_diff / 2) or last_run)
                ],
                ["count", "email"]
            )

            try_again = False
            for problem in problems:
                if please_stop:
                    break

                #FIND MOST LIKELY MATCH
                solution_agg = Multiset(allow_negative=True)
                for bug_id, agg in iteritems(self.bugs):
                    if agg.dic.get(problem.email, 0) < 0:  #ONLY BUGS THAT ARE EXPERIENCING THIS problem
                        solution_agg += agg
                solutions = jx.sort([{"email": e, "count": c} for e, c in iteritems(solution_agg.dic)], [{"field": "count", "sort": -1}, "email"])

                if last_run and len(solutions) == 2 and solutions[0].count == -solutions[1].count:
                    #exact match
                    pass
                elif len(solutions) <= 1 or (solutions[1].count + minimum_diff >= solutions[0].count):
                    #not distinctive enough
                    continue

                best_solution = solutions[0]
                Log.note(
                    "{{problem}} ({{score}}) -> {{solution}} {{matches}}",
                    problem= problem.email,
                    score= problem.count,
                    solution= best_solution.email,
                    matches= value2json(jx.select(solutions, "count")[:10:])
                )
                try_again = True
                self.add_alias(problem.email, best_solution.email)

        self.save_aliases()

    def get_canonical(self, email):
        """
        RETURN CANONICAL, OR email
        :param email:
        :return:
        """
        record = self.aliases.get(email.lower())
        if record:
            return record["canonical"]
        else:
            return email

    def add_alias(self, lost, found):
        if not found.strip():
            Log.error("expecting email")

        lost = lost.lower()
        found = found.lower()
        old_email = self.get_canonical(lost)
        new_email = self.get_canonical(found)

        delete_list = []

        #FOLD bugs ON lost=found
        for bug_id, agg in iteritems(self.bugs):
            v = agg.dic.get(lost, 0)
            if v != 0:
                agg.add(lost, -v)
                agg.add(found, v)

            if not agg:
                delete_list.append(bug_id)

        # FOLD bugs ON old_email == new_email
        if old_email != lost:
            for bug_id, agg in iteritems(self.bugs):
                v = agg.dic.get(old_email, 0)
                if v != 0:
                    agg.add(old_email, -v)
                    agg.add(new_email, v)

                if not agg:
                    delete_list.append(bug_id)

        for d in delete_list:
            del self.bugs[d]

        # FOLD ALIASES  email -> old_email GETS CHANGED TO email -> new_email
        reassign = [(lost, new_email)]
        Log.note("ALIAS MAPPED: {{alias}} -> {{new}}", alias=lost, new=new_email)
        for k, v in self.aliases.items():
            if v["canonical"] == old_email:
                if k != v["canonical"]:
                    Log.note("ALIAS REMAPPED: {{alias}} -> {{old}} -> {{new}}", alias=k, old=v["canonical"], new=found)

                reassign.append((k, new_email))

        for k, found in reassign:
            self.aliases[k] = {"canonical": new_email, "dirty": True}

    def load_aliases(self):
        try:
            if self.kwargs.elasticsearch:
                cluster= Cluster(self.kwargs.elasticsearch)
                self.es = cluster.get_or_create_index(
                    kwargs=self.kwargs.elasticsearch,
                    schema=ALIAS_SCHEMA,
                    limit_replicas=True
                )
                self.es.add_alias(self.kwargs.elasticsearch.index)

                file_date = os.path.getmtime(File(self.kwargs.file).abspath)
                index_date = float(cluster.get_metadata().indices[self.es.settings.index].settings.index.creation_date)/1000

                if file_date>index_date:
                    # LOAD FROM FILE IF THE CLUSTER IS A BIT EMPTY
                    self.es = cluster.create_index(
                        kwargs=self.kwargs.elasticsearch,
                        schema=ALIAS_SCHEMA,
                        limit_replicas=True
                    )
                    self.es.add_alias(self.kwargs.elasticsearch.index)
                    cluster.delete_all_but(self.es.settings.alias, self.es.settings.index)
                    self._load_aliases_from_file()
                    return

                esq = jx_elasticsearch.new_instance(self.es.settings)
                result = esq.query({
                    "from": "bug_aliases",
                    "select": ["canonical", "alias"],
                    "where": {"missing": "ignore"},
                    "format": "list",
                    "limit": 50000
                })
                for r in result.data:
                    self.aliases[r.alias] = {"canonical": r.canonical, "dirty": False}

                num = len(self.aliases.keys())
                Log.note("{{num}} aliases loaded from ES", num=num)

                # LOAD THE NON-MATCHES
                result = esq.query({
                    "from": "bug_aliases",
                    "select": ["canonical", "alias"],
                    "where": {"exists": "ignore"},
                    "format": "list"
                })
                for r in result.data:
                    self.not_aliases[r.alias] = r.canonical
            else:
                self._load_aliases_from_file()
        except Exception as e:
            Log.warning("Can not load aliases", cause=e)

    def _load_aliases_from_file(self):
        if self.kwargs.file:
            data = json2value(zip2bytes(File(self.kwargs.file).read_bytes()).decode('utf8'), flexible=False, leaves=False)
            self.aliases = {a: {"canonical": c, "dirty": True} for a, c in data.aliases.items()}
            self.not_aliases = data.not_aliases
            Log.note("{{num}} aliases loaded from file", num=len(self.aliases.keys()))

    def save_aliases(self):
        if self.es:
            records = []
            for k, v in self.aliases.items():
                if v["dirty"]:
                    records.append({"id": k, "value": {"canonical": v["canonical"], "alias": k}})

            if records:
                Log.note("Net new aliases saved: {{num}}", num=len(records))
                self.es.extend(records)
        elif self.kwargs.file:
            def compact():
                return {
                    "aliases": {a: c['canonical'] for a, c in self.aliases.items() if c['canonical'] != a},
                    "not_aliases": self.not_aliases
                }

            data = compact()
            File(self.kwargs.file).write_bytes(bytes2zip(value2json(data, pretty=True).encode('utf8')))
            if DEBUG:
                Log.note("verify alias file")
                self.load_aliases()
                from_file = compact()
                assertAlmostEqual(from_file, data)
                assertAlmostEqual(data, from_file)




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
    return set([s.strip().lower() for s in value.split(",") if s.strip() != ""])


def start():
    try:
        kwargs = startup.read_settings()
        constants.set(kwargs.constants)
        Log.start(kwargs.debug)
        full_analysis(kwargs)
    except Exception as e:
        Log.error("Can not start", e)
    finally:
        Log.stop()


ALIAS_SCHEMA = {
    "kwargs": {"index": {
        "number_of_shards": 3,
        "number_of_replicas": 2
    }},
    "mappings": {
        "alias": {
            "_all": {
                "enabled": False
            },
            "properties": {
            }
        }
    }
}


if __name__ == "__main__":
    start()


