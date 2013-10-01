
Bugzilla-ETL
============

Python version of Metric's Bugzilla ETL (https://github.com/mozilla-metrics/bugzilla_etl)

Motivation and Details
----------------------

[https://wiki.mozilla.org/Auto-tools/Projects/PublicES](https://wiki.mozilla.org/Auto-tools/Projects/PublicES)

Requirements
------------

  * Python 2.7
  * A MySQL/Maria database with Mozilla's Bugzilla schema ([old public version can be found here](http://people.mozilla.com/~mhoye/bugzilla/))
  * A timezone database ([instructions](./tests/resources/mySQL/README.md))
  * An ElasticSearch (v 0.20.5) cluster to hold the bug version documents

Installation
------------

Clone from Github:

    git clone https://github.com/klahnakoski/Bugzilla-ETL.git

Install requirements:

    pip install -r requirements.txt

It is best you install on Linux, but if you do install on Windows you can find 
further Windows-specific Python installation instructions at one of my other projects: [https://github.com/klahnakoski/pyLibrary/blob/master/README.md](https://github.com/klahnakoski/pyLibrary/blob/master/README.md)


Setup
-----

You must prepare a ```settings.json``` file to reference the resources, and it's filename must be provided as an argument in the command line (see [example command line script](resources/scripts/bz_etl.bat)). Examples of settings files can be found in [resources/settings](resources/settings)

Running Tests
-------------

You can run the functional tests, but you must

  * Have MySQL installed (no Bugzilla schema required)
  * Have timezone database installed ([instructions](./tests/resources/mySQL/README.md))
  * A complete ```test_settings.json``` file to point to the resources ([example](./resources/settings/test_settings_example.json))
  * Use pypy for 4x the speed: ```pypy .\tests\test_etl.py --settings=test_settings.json```



