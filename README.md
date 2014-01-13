
Bugzilla-ETL
============

Python version of Metric's Bugzilla ETL (https://github.com/mozilla-metrics/bugzilla_etl)

Motivation and Details
----------------------

[https://wiki.mozilla.org/Auto-tools/Projects/PublicES](https://wiki.mozilla.org/Auto-tools/Projects/PublicES)

Requirements
------------

  * PyPy >= 2.1.0 using Python 2.7  (PyPy version must have fix https://bugs.pypy.org/issue1392 (applied June2013))
  * A MySQL/Maria database with Mozilla's Bugzilla schema ([old public version can be found here](http://people.mozilla.com/~mhoye/bugzilla/))
  * A timezone database ([instructions](./tests/resources/mySQL/README.md))
  * An ElasticSearch (v 0.20.5) cluster to hold the bug version documents

Installation
------------

Python and SetupTools are required.  It is best you install on Linux, but if you do install on Windows please [follow instructions to get these installed](https://github.com/klahnakoski/pyLibrary#windows-7-install-instructions-for-python).  When done, installation is easy:

    git clone https://github.com/klahnakoski/Bugzilla-ETL.git

then install requirements:

    cd Bugzilla-ETL
    pip install -r requirements.txt

**WARNING: ```pip install Bugzilla-ETL``` does not work** - I have been unable to get Pip to install resource files consistently across platforms and Python versions.

Installation with PyPy
----------------------

PyPy will execute 4 to 5 times faster then CPython.  PyPy maintains its own environment, and it's own version of the module binaries.  This means running SetupTools is just a little different.  After

    git clone https://github.com/klahnakoski/Bugzilla-ETL.git

then install requirements with PyPy's version of pip:

    cd Bugzilla-ETL
    c:\PyPy27\bin\pip.exe install -r requirements.txt

Despite my Windows example, the equivalent must be done in Linux.


Setup
-----

You must prepare a ```settings.json``` file to reference the resources,
and it's filename must be provided as an argument in the command line.
Examples of settings files can be found in [resources/settings](resources/settings)

Bugzilla-ETL keeps local run state in the form of two files:
```first_run_time``` and ```last_run_time```.  These are both parameters
in the ``settings.json``` file.

  * ```first_run_time``` is written only if it does not exist, and triggers a full ETL refresh.  Delete this file if you want to create a new ES index and start ETL from the beginning.
  * ```last_run_time``` is recorded whenever there has been a successful ETL.  This file will not exist until the initial full ETL has completed successfully.  Deleteing this file should have no net effect, other than making the program work harder then it should.

Running bz_etl.py
------------------

Asuming your ```settings.json``` file is in ```~/Bugzilla_ETL```:

    cd ~/Bugzilla_ETL

    pypy bzETL\bz_etl.py --settings=settings.json

Use ```--help``` for more options, and see [example command line script](resources/scripts/bz_etl.bat)

Got it working?
--------------

The initial ETL will take over two hours.  If you want something
quicker to confirm your configuration is correct, use ```--reset
--quick``` arguments on the command line.   This will limit ETL
to the first 1000, and last 1000 bugs.

    cd ~/Bugzilla_ETL
    pypy bzETL\bz_etl.py  --settings=settings.json --reset --quick

Using Cron
----------

Bugzilla-ETL is meant to be triggered by cron; usually every 10 minutes.
Bugzilla-ETL limits itself to only one instance *per ```settings.json```
file*:  That way, if more then one instance is accidentally run, the
subsequent instances will do no work and shutdown cleanly.

Running Tests
-------------

The Git clone will include test code.  You can run those tests, but you must...

  * Have MySQL installed (no Bugzilla schema required)
  * Have timezone database installed ([instructions](./tests/resources/mySQL/README.md))
  * A complete ```test_settings.json``` file to point to the resources ([example](./resources/settings/test_settings.json))
  * Use pypy for 4x the speed: ```pypy .\tests\test_etl.py --settings=test_settings.json```

Upgrades
--------

There may be enhancements from time to time.  To get them

    cd ~/Bugzilla-ETL
    git pull origin master
    pip install -r requirements.txt

After upgrading the code, you may want to trigger a full ETL.  To do this,
you may either

1.  run ```bz_etl.py``` with the ```--reset--``` flag directly, or
2.  remove the ```first_run_time``` file (and the next cron event will tigger a full ETL)


More on ElasticSearch
---------------------

If you are new to ElasticSearch, I recommend using [ElasticSearch Head](https://github.com/mobz/elasticsearch-head)
for getting cluster status, current schema definitions, viewing individual
records, and more.  Clone it off of GitHub, and open the ```index.html``` file
from in your browser.  Here are some alternate [instructions](http://mobz.github.io/elasticsearch-head/).
