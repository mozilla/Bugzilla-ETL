from bzETL.util.db import DB, SQL
from bzETL.util.logs import Log
from bzETL.util.startup import startup

def main():
    """
    MEANT TO BE RUN JUST ONCE IN DEVELOPMENT TO CONVERT A BIG PUBLIC
    DATABASE (8G+) INTO A TINY TESTING DB (FOR ADDING TO REPOSITORY)
    """
    try:
        settings=startup.read_settings()
        Log.start(settings.debug)
        input=raw_input("We are going to totally wipe out the "+settings.bugzilla.schema.upper()+" schema at "+settings.bugzilla.host.upper()+"!  Type \"YES\" to continue: ")
        if input!="YES":
            Log.note("Aborted.  No Changes made.")
            return

        Log.note("Scrubbing db of those pesky records.")
        Log.note("This is going to take hours ...")

        DB.execute_file(settings.bugzilla, "./tests/resources/sql/scrub_db.sql", {"bug_list":SQL(settings.param.bugs)})
        Log.note("... Done!")
    finally:
        Log.stop()

main()