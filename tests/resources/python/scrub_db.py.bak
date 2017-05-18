# encoding: utf-8
#
from pyLibrary.sql.db import MySQL, SQL
from pyLibrary.env.logs import Log
from pyLibrary.env import startup

def main():
    """
    MEANT TO BE RUN JUST ONCE IN DEVELOPMENT TO CONVERT A BIG PUBLIC
    DATABASE (8G+) INTO A TINY TESTING MySQL (FOR ADDING TO REPOSITORY)
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

        MySQL.execute_file(settings.bugzilla, "./tests/resources/sql/scrub_db.sql", {
            "schema":settings.bugzilla.schema,
            "bug_list":SQL(settings.param.bugs)
        })
        Log.note("... Done!")
    finally:
        Log.stop()

main()
