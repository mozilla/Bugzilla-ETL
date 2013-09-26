Bugzilla-ETL
============

Python version of Metric's Bugzilla ETL (https://github.com/mozilla-metrics/bugzilla_etl)

Motivation and Details
----------------------

[https://wiki.mozilla.org/Auto-tools/Projects/PublicES](https://wiki.mozilla.org/Auto-tools/Projects/PublicES)

Installation
------------

Clone from Github:

    git clone https://github.com/klahnakoski/Bugzilla-ETL.git

Install requirements:

    pip install -r requirements.txt

This code is based on a copy of my [pyLibrary](https://github.com/klahnakoski/pyLibrary).  It has further instructions for getting Python, and it's modules, running on Windows:
[https://github.com/klahnakoski/pyLibrary/blob/master/README.md](https://github.com/klahnakoski/pyLibrary/blob/master/README.md)


Setup
-----

You will also require:

  * A MySQL/Maria database with Mozilla's Bugzilla schema ([old public version can be found here](http://people.mozilla.com/~mhoye/bugzilla/))
  * An ElasticSearch cluster to hold the bug version documents
  * A ```settings.json``` file to connect everything together


The JSON file of settings must be provided as an
argument in the command line (see [example command line script](resources/scripts/bz_etl.bat)). Examples of settings files can be found in [resources/settings](resources/settings)

Running Tests
-------------

You can run the functional tests, but you must

  * Have MySQL installed, along with the timezone database ([instructions](./tests/resources/mySQL/README.md))
  * A complete ```test_settings.json``` file to point to the resources ([example](./resources/settings/test_settings_example.json))
  * Use pypy for 4x the speed!!

Bugzilla-ETL
============

Python version of Metric's Bugzilla ETL (https://github.com/mozilla-metrics/bugzilla_etl)

Motivation and Details
----------------------

[https://wiki.mozilla.org/Auto-tools/Projects/PublicES](https://wiki.mozilla.org/Auto-tools/Projects/PublicES)

Installation
------------

Clone from Github:

    git clone https://github.com/klahnakoski/Bugzilla-ETL.git

Install requirements:

    pip install -r requirements.txt

This code is based on a copy of my [pyLibrary](https://github.com/klahnakoski/pyLibrary).  It has further instructions for getting Python, and it's modules, running on Windows:
[https://github.com/klahnakoski/pyLibrary/blob/master/README.md](https://github.com/klahnakoski/pyLibrary/blob/master/README.md)


Setup
-----

You will also require:

  * A MySQL/Maria database with Mozilla's Bugzilla schema ([old public version can be found here](http://people.mozilla.com/~mhoye/bugzilla/))
  * An ElasticSearch cluster to hold the bug version documents
  * A ```settings.json``` file to connect everything together


The JSON file of settings must be provided as an
argument in the command line (see [example command line script](resources/scripts/bz_etl.bat)). Examples of settings files can be found in [resources/settings](resources/settings)

Running Tests
-------------

You can run the functional tests, but you must

  * Have MySQL installed, along with the timezone database ([instructions](./tests/resources/mySQL/README.md))
  * A complete ```test_settings.json``` file to point to the resources ([example](./resources/settings/test_settings_example.json))
  * Use pypy for 4x the speed: ```pypy .\tests\test_etl.py --settings=test_settings.json```



