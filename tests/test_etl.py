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
from util.query import Q
from util.startup import startup
from util.strings import json_scrub
from util.struct import Struct


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

        File(settings.param.errors).delete()
        for bug_id in settings.param.bugs:
            versions=Q.sort(get_all_bug_versions(candidate, bug_id, datetime.utcnow()), "modified_ts")
            # WE CAN NOT EXPECT candidate TO BE UP TO DATE BECAUSE IT IS USING AN OLD IMAGE
            max_time=CNV.unixmilli2datetime(versions[-1].modified_ts)
            ref_versions=Q.sort(map(compare_es.old2new, get_all_bug_versions(reference, bug_id, max_time)), "modified_ts")

            can=json.dumps(json_scrub(versions), indent=4, sort_keys=True, separators=(',', ': ') )
            ref=json.dumps(json_scrub(ref_versions), indent=4, sort_keys=True, separators=(',', ': ') )
            if can!=ref:
                File(settings.param.errors+"/try/"+str(bug_id)+".txt").write(can)
                File(settings.param.errors+"/exp/"+str(bug_id)+".txt").write(ref)

            

def test_etl():
    try:
        settings=startup.read_settings()
        D.start(settings.debug)
        main(settings)
    finally:
        D.stop()

test_etl()