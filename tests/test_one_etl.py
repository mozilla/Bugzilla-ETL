#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from bzETL.util.db import all_db
from bzETL.util.logs import Log
from bzETL.util import startup
from test_etl import test_specific_bugs


def main():
    try:
        settings=startup.read_settings()
        Log.start(settings.debug)

        test_specific_bugs(settings)

        if all_db:
            Log.error("not all db connections are closed")

        Log.note("All tests pass!  Success!!")
    finally:
        Log.stop()



def profile_etl():
    import profile
    import pstats
    filename="profile_stats.txt"

    profile.run("""
try:
    main()
except Exception, e:
    pass
    """, filename)
    p = pstats.Stats(filename)
    p.strip_dirs().sort_stats("tottime").print_stats(40)




if __name__=="__main__":
    profile_etl()
    # main()
