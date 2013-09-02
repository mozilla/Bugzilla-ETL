################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################

# MAKE FAKE DATABASE

# FILL WITH KNOW BUGS

# MAKE FAKE ES

# PULL ALL BUGS OUT OF FAKE DATABASE

# COMPARE TO EXPECTED IN FAKE ES
from datetime import datetime
import json
from bz_etl import etl
import compare_es
from extract_bugzilla import get_bugs_table_columns
from compare_es import get_all_bug_versions
from fake_es import Fake_ES
import transform_bugzilla
from util.cnv import CNV
from util.db import DB, SQL
from util.debug import D
from util.elasticsearch import ElasticSearch
from util.files import File
from util.maths import Math
from util.query import Q
from util.randoms import Random
from util.startup import startup
from util.strings import json_scrub
from util.struct import Struct
from util.timer import Timer


def main(settings):

    #MAKE HANDLES TO CONTAINERS
    with DB(settings.bugzilla) as db:
        #REAL ES
#        if settings.candidate.alias is None:
#            settings.candidate.alias=settings.candidate.index
#            settings.candidate.index=settings.candidate.alias+CNV.datetime2string(datetime.utcnow(), "%Y%m%d_%H%M%S")
#        candidate=ElasticSearch.create_index(settings.candidate, File(settings.candidate.schema_file).read())
        candidate=Fake_ES(settings.fake_es)

        reference=ElasticSearch(settings.reference)

        #SETUP RUN PARAMETERS
        param=Struct()
        param.BUGS_TABLE_COLUMNS=get_bugs_table_columns(db, settings.bugzilla.schema)
        param.BUGS_TABLE_COLUMNS_SQL=SQL(",\n".join(["`"+c.column_name+"`" for c in param.BUGS_TABLE_COLUMNS]))
        param.BUGS_TABLE_COLUMNS=Q.select(param.BUGS_TABLE_COLUMNS, "column_name")
        param.END_TIME=CNV.datetime2milli(datetime.utcnow())
        param.START_TIME=0
        param.alias_file=settings.param.alias_file
        param.BUG_IDS_PARTITION=SQL("bug_id in {{bugs}}", {"bugs":db.quote(settings.param.bugs)})

        etl(db, candidate, param)

        #COMPARE ALL BUGS
        compare_both(candidate, reference, settings, settings.param.bugs)



#PICK SOME BUGS BY RANDOM
def random_sample_of_bugs(settings):
    NUM_TO_TEST=100
    MAX_BUG_ID=900000


    with DB(settings.bugzilla) as db:
        candidate=Fake_ES(settings.fake_es)
        reference=ElasticSearch(settings.reference)

        #GO FASTER BY STORING LOCAL FILE
        local_cache=File(settings.param.temp_dir+"/private_bugs.json")
        if local_cache.exists:
            private_bugs=set(CNV.JSON2object(local_cache.read()))
        else:
            with Timer("get private bugs"):
                private_bugs= compare_es.get_private_bugs(reference)
                local_cache.write(CNV.object2JSON(private_bugs))

        while True:
            some_bugs=[b for b in [Random.int(MAX_BUG_ID) for i in range(NUM_TO_TEST)] if b not in private_bugs]

            #SETUP RUN PARAMETERS
            param=Struct()
            param.BUGS_TABLE_COLUMNS=get_bugs_table_columns(db, settings.bugzilla.schema)
            param.BUGS_TABLE_COLUMNS_SQL=SQL(",\n".join(["`"+c.column_name+"`" for c in param.BUGS_TABLE_COLUMNS]))
            param.BUGS_TABLE_COLUMNS=Q.select(param.BUGS_TABLE_COLUMNS, "column_name")
            param.END_TIME=CNV.datetime2milli(datetime.utcnow())
            param.START_TIME=0
            param.alias_file=settings.param.alias_file
            param.BUG_IDS_PARTITION=SQL("bug_id in {{bugs}}", {"bugs":db.quote(some_bugs)})

            try:
                etl(db, candidate, param)

                #COMPARE ALL BUGS
                found_errors=compare_both(candidate, reference, settings, some_bugs)
                if found_errors:
                    D.println("Errors found")
                    break
                else:
                    pass
            except Exception, e:
                D.warning("Total faiure during compare of bugs {{bugs}}", {"bugs":some_bugs}, e)


#COMPARE ALL BUGS
def compare_both(candidate, reference, settings, some_bugs):
    File(settings.param.errors).delete()
    for bug_id in some_bugs:
        versions = Q.sort(
            get_all_bug_versions(candidate, bug_id, datetime.utcnow()),
            "modified_ts")
        # WE CAN NOT EXPECT candidate TO BE UP TO DATE BECAUSE IT IS USING AN OLD IMAGE
        if len(versions)==0:
            max_time=datetime.utcnow()
        else:
            max_time = CNV.milli2datetime(versions[-1].modified_ts)
            
        ref_versions = Q.sort(map(compare_es.old2new, get_all_bug_versions(reference, bug_id, max_time)), "modified_ts")

        can = json.dumps(json_scrub(versions), indent=4, sort_keys=True, separators=(',', ': '))
        ref = json.dumps(json_scrub(ref_versions), indent=4, sort_keys=True, separators=(',', ': '))
        found_errors=False
        if can != ref:
            found_errors=True
            File(settings.param.errors + "/try/" + str(bug_id) + ".txt").write(can)
            File(settings.param.errors + "/exp/" + str(bug_id) + ".txt").write(ref)

    return found_errors

def test_etl():
    try:
        settings=startup.read_settings()
        D.start(settings.debug)

#        random_sample_of_bugs(settings)
        main(settings)
    finally:
        D.stop()




test_etl()