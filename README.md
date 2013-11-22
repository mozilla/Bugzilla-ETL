
Bugzilla-ETL
============

Python version of Metric's Bugzilla ETL (https://github.com/mozilla-metrics/bugzilla_etl)

Motivation and Details
----------------------

[https://wiki.mozilla.org/Auto-tools/Projects/PublicES](https://wiki.mozilla.org/Auto-tools/Projects/PublicES)

Requirements
------------

  * PyPy 2.1.0 using Python 2.7  (cPython is way too slow)
  * A MySQL/Maria database with Mozilla's Bugzilla schema ([old public version can be found here](http://people.mozilla.com/~mhoye/bugzilla/))
  * A timezone database ([instructions](./tests/resources/mySQL/README.md))
  * An ElasticSearch (v 0.20.5) cluster to hold the bug version documents

Installation
------------

PyPy and SetupTools are required.  If you are installing on Windows please [follow instructions to get these installed](https://github.com/klahnakoski/pyLibrary#windows-7-install-instructions-for-python).  When done, installation is easy:

    pip install Bugzilla-ETL

Running bz_etl.py
------------------

You must prepare a ```settings.json``` file to reference the resources, and it's filename must be provided as an argument in the command line. Examples of settings files can be found in [resources/settings](resources/settings)

  * Set working directory to ```~/Bugzilla_ETL/```
  * Set ```PYTHONPATH=.```
  * Exceute ```pypy .\bzETL\bz_etl.py --settings=settings.json``` (also see [example command line script](resources/scripts/bz_etl.bat))

Bugzille-ETL keeps local run state in the form of two files: ```first_run_time``` and ```last_run_time```.  These are both parameters in the ``settings.json``` file.

  * ```first_run_time``` is written only if it does not exist, and triggers a full ETL refresh.  Delete this file if you want to create a new ES index and start ETL from the beginning.
  * ```last_run_time``` is recorded whenever there has been a successful ETL.  This file will not exist until the initial full ETL has completed successfully.  Deleteing this file should have no net effect, other than making the program work harder then it should.

Does it Work?
--------------

The initial ETL will take over two hours.  If you want a quicker test to confirm your configuration is correct, use "--quick" argument on the command line.   This will limit ETL to the first 1000, and last 1000 bugs.

    ```pypy .\bzETL\bz_etl.py --settings=settings.json --quick```


Developer Installation
----------------------

If you plan to help improve this software, or if you enjoy working from source, you can clone from Github:

    git clone https://github.com/klahnakoski/Bugzilla-ETL.git

Install requirements:

    pip install -e

It is best you install on Linux, but if you do install on Windows you can find
further Windows-specific Python installation instructions at one of my other projects: [https://github.com/klahnakoski/pyLibrary/blob/master/README.md](https://github.com/klahnakoski/pyLibrary/blob/master/README.md)

Running Tests
-------------

You can run the functional tests, but you must

  * Have MySQL installed (no Bugzilla schema required)
  * Have timezone database installed ([instructions](./tests/resources/mySQL/README.md))
  * A complete ```test_settings.json``` file to point to the resources ([example](./resources/settings/test_settings_example.json))
  * Use pypy for 4x the speed: ```pypy .\tests\test_etl.py --settings=test_settings.json```



More on ElasticSearch
---------------------

If you are new to ElasticSearch, I recommend using [ElasticSearch Head](https://github.com/mobz/elasticsearch-head)
for getting cluster status, current schema definitions, viewing individual
records, and more.  Clone it off of GitHub, and open the ```index.html``` file
from in your browser.  Here are some alternate [instructions](http://mobz.github.io/elasticsearch-head/).
