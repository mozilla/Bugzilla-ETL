from bzETL.util.db import DB
from bzETL.util.logs import Log
from bzETL.util.startup import startup

def main():
    """
    MEANT TO BE RUN JUST ONCE IN DEVELOPMENT TO CONVERT A BIG PUBLIC DATABASE 8G+ INTO
    A TINY TESTING DB (FOR ADDING TO REPOSITORY)
    """
    try:
        settings=startup.read_settings("test_settings.json")
        Log.start(settings.debug)
        DB.execute_file(settings.bugzilla, "./tests/resources/sql/scrub_db.sql", {"bug_list":settings.param.bugs})
    finally:
        Log.stop()

main()