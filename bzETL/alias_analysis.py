from bzETL.extract_bugzilla import get_all_cc_changes
from bzETL.util import startup, struct
from bzETL.util.cnv import CNV
from bzETL.util.db import DB
from bzETL.util.files import File
from bzETL.util.logs import Log
from bzETL.util.multiset import Multiset
from bzETL.util.queries import Q
from bzETL.util.struct import nvl, Struct, Null

bugs={}
aliases={}


def main(settings, bug_list=None, please_stop=None, restart=False):
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

    if not restart:
        loadAliases(settings)

    if bug_list:
        with DB(settings.bugzilla) as db:
            data = get_all_cc_changes(db, bug_list)
            aggregator(data)
            analysis(settings, True, please_stop)
        return


    with DB(settings.bugzilla) as db:
        start = nvl(settings.param.start, 0)
        end = nvl(settings.param.end, db.query("SELECT max(bug_id)+1 bug_id FROM bugs")[0].bug_id)

        #Perform analysis on blocks of bugs, in case we crash partway through
        for s in range(start, end, settings.param.alias_increment):
            e = s + settings.param.alias_increment
            Log.note("Load range {{start}}-{{end}}", {
                "start": s,
                "end": e
            })
            data = get_all_cc_changes(db, range(s, e))
            if please_stop:
                break
            aggregator(data)

            analysis(settings, e>=end, please_stop)


def split_email(value):
    if not value:
        return set()

    if value.startswith("?") or value.endswith("?"):
        return set()
    return set([s.strip() for s in value.split(",") if s.strip()!=""])


def aggregator(data):
    """
    FLATTEN CC LISTS OVER TIME BY BUG
    MULTISET COUNTS THE NUMBER OF EMAIL AT BUG CREATION
    NEGATIVE MEANS THERE WAS AN ADD WITHOUT A REMOVE (AND NOT IN CURRENT LIST)
    """
    for d in data:
        new_emails=Q.map(split_email(d.new_value), alias)
        old_emails=Q.map(split_email(d.old_value), alias)

        for e in new_emails | old_emails:
            details=aliases.get(e, Struct())
            aliases[e]=details

        agg = bugs.get(d.bug_id, Multiset(allow_negative=True))
        agg = agg - new_emails
        agg = agg + old_emails
        bugs[d.bug_id] = agg


def analysis(settings, last_run, please_stop):
    DIFF=7
    if last_run:
        DIFF=4      #ONCE WE HAVE ALL THE DATA IN WE CAN BE LESS DISCRIMINATING
    try_again = True

    while try_again and not please_stop:
        #FIND EMAIL MOST NEEDING REPLACEMENT
        problem_agg=Multiset(allow_negative=True)
        for bug_id, agg in bugs.iteritems():
            #ONLY COUNT NEGATIVE EMAILS
            for email, count in agg.dic.iteritems():
                if count<0:
                    problem_agg.add(alias(email), amount=count)

        problems = Q.sort([
            {"email": e, "count": c}
            for e, c in problem_agg.dic.iteritems()
            if not aliases.get(e, Null).ignore and (c <= -(DIFF / 2) or last_run)
        ], ["count", "email"])

        try_again=False
        for problem in problems:
            if please_stop:
                break

            #FIND MOST LIKELY MATCH
            solution_agg=Multiset(allow_negative=True)
            for bug_id, agg in bugs.iteritems():
                if agg.dic.get(problem.email, 0)<0:  #ONLY BUGS THAT ARE EXPERIENCING THIS problem
                    solution_agg += agg
            solutions = Q.sort([{"email": e, "count": c} for e, c in solution_agg.dic.iteritems()], [{"field":"count", "sort":-1}, "email"])

            if last_run and len(solutions)==2 and solutions[0].count==-solutions[1].count:
                #exact match
                pass
            elif len(solutions)<=1 or (solutions[1].count+DIFF>=solutions[0].count):
                #not distinctive enough
                continue

            best_solution=solutions[0]
            Log.note("{{problem}} ({{score}}) -> {{solution}} {{matches}}",{
                "problem":problem.email,
                "score":problem.count,
                "solution":best_solution.email,
                "matches":CNV.object2JSON(Q.select(solutions, "count")[:10])
            })
            try_again=True
            add_alias(problem.email, best_solution.email)

    saveAliases(settings)


def alias(email):
    output = nvl(aliases.get(email, Null).canonical, email)
    return output


def add_alias(lost, found):
    found_record = aliases.get(found, None)
    lost_record = aliases.get(lost, None)

    new_canonical = found
    old_canonical = nvl(lost_record.canonical, lost)
    lost_record.canonical = new_canonical

    delete_list=[]

    #FOLD bugs ON lost=found
    for bug_id, agg in bugs.iteritems():
        v=agg.dic.get(lost, 0)
        if v!=0:
            agg.add(lost, -v)
            agg.add(found, v)

        if not agg:
            delete_list.append(bug_id)

    #FOLD bugs ON old_canonical=new_canonical
    if old_canonical!=lost:
        for bug_id, agg in bugs.iteritems():
            v=agg.dic.get(old_canonical, 0)
            if v!=0:
                agg.add(old_canonical, -v)
                agg.add(new_canonical, v)

            if not agg:
                delete_list.append(bug_id)

    for d in delete_list:
        del bugs[d]

    #FOLD ALIASES
    for k, v in aliases.iteritems():
        if v.canonical == old_canonical:
            Log.note("ALIAS REMAPPED: {{alias}}->{{old}} to {{alias}}->{{new}}",{
                "alias":k,
                "old":old_canonical,
                "new":found
            })
            v.canonical = found

def loadAliases(settings):
    try:
        try:
            alias_json = File(settings.param.alias_file).read()
        except Exception, e:
            alias_json = "{}"
        #self.aliases IS A dict POINTING TO structs
        for k, v in CNV.JSON2object(alias_json).iteritems():
            aliases[k]=struct.wrap(v)

        Log.note("{{num}} aliases loaded", {"num": len(aliases.keys())})
    except Exception, e:
        Log.error("Can not init aliases", e)


def saveAliases(settings):

    compressed = {
        email: details
        for email, details in aliases.iteritems()
        if details.canonical
    }

    #COMPARE WITH PREVIOUS ALIAS VERSION
    try:
        old_alias_json = File(settings.param.alias_file).read()
    except Exception, e:
        old_alias_json = "{}"
    old_aliases={}
    for k, v in CNV.JSON2object(old_alias_json).iteritems():
        old_aliases[k]=struct.wrap(v)

    added = set(compressed.keys()) - set(old_aliases.keys())
    removed = set(old_aliases.keys()) - set(compressed.keys())
    common = set(compressed.keys()) & set(old_aliases.keys())

    changed=set()
    for c in common:
        if CNV.object2JSON(compressed[c], pretty=True) != CNV.object2JSON(old_aliases[c], pretty=True):
            changed.add(c)

    if added or removed or changed:
        alias_json = CNV.object2JSON(compressed, pretty=True)
        file = File(settings.param.alias_file)
        file.write(alias_json)

        Log.note("{{num}} of {{total}} aliases saved", {
            "num": len(compressed.keys()),
            "total": len(aliases.keys())
        })


def start():
    try:
        settings = startup.read_settings()
        Log.start(settings.debug)
        main(settings, restart=True)
    except Exception, e:
        Log.error("Can not start", e)
    finally:
        Log.stop()



if __name__=="__main__":
    start()
