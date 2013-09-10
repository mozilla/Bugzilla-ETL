from datetime import datetime
import re
from bzETL.util import struct
from bzETL.util.cnv import CNV
from bzETL.util.debug import D
from bzETL.util.maths import Math
from bzETL.util.query import Q
from bzETL.util.struct import Struct, StructList


MULTI_FIELDS = ["cc", "blocked", "dependson", "dupe_by", "dupe_of", "flags", "keywords", "bug_group", "see_also"]
NUMERIC_FIELDS=[      "blocked", "dependson", "dupe_by", "dupe_of",
    "votes",
    "estimated_time",
    "remaining_time",
    "everconfirmed"
    
]

# Used to reformat incoming dates into the expected form.
# Example match: "2012/01/01 00:00:00.000"
DATE_PATTERN_STRICT = re.compile("^[0-9]{4}[\\/-][0-9]{2}[\\/-][0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}")
DATE_PATTERN_STRICT_SHORT = re.compile("^[0-9]{4}[\\/-][0-9]{2}[\\/-][0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}")
# Example match: "2012-08-08 0:00"
DATE_PATTERN_RELAXED = re.compile("^[0-9]{4}[\\/-][0-9]{2}[\\/-][0-9]{2}")


#WE ARE RENAMING THE ATTACHMENTS FIELDS TO CAUSE LESS PROBLEMS IN ES QUERIES
def rename_attachments(bug_version):
    if bug_version.attachments is None: return bug_version
    bug_version.attachments=CNV.JSON2object(CNV.object2JSON(bug_version.attachments).replace("attachments.", "attachments_"))
    return bug_version



#NORMALIZE BUG VERSION TO STANDARD FORM
def normalize(bug):
    bug.id=unicode(bug.bug_id)+"_"+unicode(bug.modified_ts)[:-3]
    bug._id=None

    #ENSURE STRUCTURES ARE SORTED
    # Do some processing to make sure that diffing between runs stays as similar as possible.
    bug.flags=Q.sort(bug.flags, "value")

    if bug.attachments is not None:
        bug.attachments=Q.sort(bug.attachments, "attach_id")
        for a in bug.attachments:
            a.flags=Q.sort(a.flags, "value")

    bug.changes=Q.sort(bug.changes, ["attach_id", "field_name"])

    #bug IS CONVERTED TO A 'CLEAN' COPY
    bug=scrub(bug)

    for f in NUMERIC_FIELDS:
        v=bug[f]
        if v is None: continue
                
        if f in MULTI_FIELDS:
            bug[f]=CNV.value2intlist(v)
        elif v==0:
            del bug[f]
        

    # Also reformat some date fields
    for dateField in ["deadline", "cf_due_date", "cf_last_resolved"]:
        v=bug[dateField]
        if v is None: continue
        try:
            if isinstance(v, datetime):
                bug[dateField] = CNV.datetime2milli(v)
            elif isinstance(v, long) and len(unicode(v))==13:
                bug[dateField]=v
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
            D.error("problem with converting date to milli (value={{value}})", {"value":bug[dateField]}, e)





    return bug




#REMOVE KEYS OF DEGENERATE VALUES (EMPTY STRINGS, EMPTY LISTS, AND NULLS)
#TO LOWER CASE
#CONVERT STRINGS OF NUMBERS TO NUMBERS
#RETURNS **COPY**, DOES NOT CHANGE ORIGINAL
def scrub(r):
    return struct.wrap(_scrub(r))

def _scrub(r):
#    if r=="1.0":
#        D.println("")

    try:
        if r is None or r=="":
            return None
        elif Math.is_number(r):
            return CNV.value2number(r)
        elif isinstance(r, basestring):
#            return r
            return r.lower()
        elif isinstance(r, dict):
            if isinstance(r, Struct): r=r.dict
            output={}
            for k, v in r.items():
                v=_scrub(v)
                if v is not None: output[k.lower()]=v
            if len(output)==0: return None
            return output
        elif hasattr(r, '__iter__'):
            if isinstance(r, StructList): r=r.list
            output=[]
            for v in r:
                v=_scrub(v)
                if v is not None: output.append(v)
            if len(output)==0: return None
            try:
                return Q.sort(output)
            except Exception:
                return output
        else:
            return r
    except Exception, e:
        D.warning("Can not scrub: {{json}}", {"json":r})


