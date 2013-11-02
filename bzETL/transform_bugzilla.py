# encoding: utf-8
#
from datetime import datetime, date
import re
from bzETL.util.cnv import CNV
from bzETL.util.elasticsearch import ElasticSearch
from bzETL.util.logs import Log
from bzETL.util.queries import Q


USE_ATTACHMENTS_DOT = True

MULTI_FIELDS = ["cc", "blocked", "dependson", "dupe_by", "dupe_of", "flags", "keywords", "bug_group", "see_also"]
NUMERIC_FIELDS=[      "blocked", "dependson", "dupe_by", "dupe_of",
    "votes",
    "estimated_time",
    "remaining_time",
    "everconfirmed",
    "uncertain"

]

# Used to reformat incoming dates into the expected form.
# Example match: "2012/01/01 00:00:00.000"
DATE_PATTERN_STRICT = re.compile("^[0-9]{4}[\\/-][0-9]{2}[\\/-][0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}")
DATE_PATTERN_STRICT_SHORT = re.compile("^[0-9]{4}[\\/-][0-9]{2}[\\/-][0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}")
# Example match: "2012-08-08 0:00"
DATE_PATTERN_RELAXED = re.compile("^[0-9]{4}[\\/-][0-9]{2}[\\/-][0-9]{2}")


#WE ARE RENAMING THE ATTACHMENTS FIELDS TO CAUSE LESS PROBLEMS IN ES QUERIES
def rename_attachments(bug_version):
    if bug_version.attachments == None: return bug_version
    if not USE_ATTACHMENTS_DOT:
        bug_version.attachments=CNV.JSON2object(CNV.object2JSON(bug_version.attachments).replace("attachments.", "attachments_"))
    return bug_version



#NORMALIZE BUG VERSION TO STANDARD FORM
def normalize(bug):
    bug=bug.copy()
    bug.id = unicode(bug.bug_id) + "_" + unicode(bug.modified_ts)[:-3]
    bug._id = None

    #ENSURE STRUCTURES ARE SORTED
    # Do some processing to make sure that diffing between runs stays as similar as possible.
    bug.flags=Q.sort(bug.flags, "value")

    if bug.attachments != None:
        if USE_ATTACHMENTS_DOT:
            bug.attachments=CNV.JSON2object(CNV.object2JSON(bug.attachments).replace("attachments_", "attachments."))
        bug.attachments = Q.sort(bug.attachments, "attach_id")
        for a in bug.attachments:
            for k,v in a.items():
                if \
                    k.endswith("isobsolete") or \
                    k.endswith("ispatch") or \
                    k.endswith("isprivate")\
                :
                    a[k.replace(".", "\.")]=CNV.value2int(v)

            a.flags = Q.sort(a.flags, ["modified_ts", "value"])

    if bug.changes != None:
        if USE_ATTACHMENTS_DOT:
            bug.changes=CNV.JSON2object(CNV.object2JSON(bug.changes).replace("attachments_", "attachments."))
        bug.changes = Q.sort(bug.changes, ["attach_id", "field_name"])

    #bug IS CONVERTED TO A 'CLEAN' COPY
    bug = ElasticSearch.scrub(bug)
    # bug.attachments = nvl(bug.attachments, [])    # ATTACHMENTS MUST EXIST


    for f in NUMERIC_FIELDS:
        v = bug[f]
        if v == None:
            continue
        elif f in MULTI_FIELDS:
            bug[f] = CNV.value2intlist(v)
        elif CNV.value2number(v) == 0:
            del bug[f]
        else:
            bug[f]=CNV.value2number(v)

    # Also reformat some date fields
    for dateField in ["deadline", "cf_due_date", "cf_last_resolved"]:
        v = bug[dateField]
        if v == None: continue
        try:
            if isinstance(v, datetime) or isinstance(v, date):
                bug[dateField] = CNV.datetime2milli(v)
            elif isinstance(v, long) and len(unicode(v)) in [12, 13]:
                bug[dateField] = v
            elif not isinstance(v, basestring):
                Log.error("situation not handled")
            elif DATE_PATTERN_STRICT.match(v):
                # Convert to "2012/01/01 00:00:00.000"
                # Example: bug 856732 (cf_last_resolved)
                # dateString = v.substring(0, 10).replace("/", '-') + "T" + v.substring(11) + "Z"
                bug[dateField] = CNV.datetime2milli(CNV.string2datetime(v+"000", "%Y/%m/%d %H:%M%:S%f"))
            elif DATE_PATTERN_STRICT_SHORT.match(v):
                # Convert "2012/01/01 00:00:00" to "2012-01-01T00:00:00.000Z", then to a timestamp.
                # Example: bug 856732 (cf_last_resolved)
                # dateString = v.substring(0, 10).replace("/", '-') + "T" + v.substring(11) + "Z"
                bug[dateField] = CNV.datetime2milli(CNV.string2datetime(v.replace("-", "/"), "%Y/%m/%d %H:%M:%S"))
            elif DATE_PATTERN_RELAXED.match(v):
                # Convert "2012/01/01 00:00:00.000" to "2012-01-01"
                # Example: bug 643420 (deadline)
                #          bug 726635 (cf_due_date)
                bug[dateField] = CNV.datetime2milli(CNV.string2datetime(v[0:10], "%Y-%m-%d"))
        except Exception, e:
            Log.error("problem with converting date to milli (value={{value}})", {"value":bug[dateField]}, e)

    bug.votes = None

    return ElasticSearch.scrub(bug)

