Bugzilla-ETL
============

Python version of Metric's Bugzilla ETL (https://github.com/mozilla-metrics/bugzilla_etl)

Motivation and Details
----------------------

[https://wiki.mozilla.org/Auto-tools/Projects/PublicES](https://wiki.mozilla.org/Auto-tools/Projects/PublicES)

Installation
------------

TODO


Setup
-----

You will require a JSON file of settings, and that file must be provided as an
argument in the command line (see [example command line script](https://github.com/klahnakoski/Bugzilla-ETL/blob/master/src/scripts/bz_etl.bat))

Here is my ```settings.json``` file:

	{
		"param":{
			"start":0,
			"increment":1000,
			"alias_file":"./data/bugzilla_aliases.txt"
		},
		"bugzilla":{
			"old.host":"localhost",
			"host":"klahnakoski-es.corp.tor1.mozilla.com",
			"port":3306,
			"username":"root",
			"password":"password",
			"schema":"bugzilla",
			"debug":false
		},
		"es":{
			"host":"http://localhost",
			"port":"9200",
			"index":"bugs",
			"type":"bug_version",
			"schema_file":"./src/json/bug_version.json"
		},
		"debug":{
			"log":{
				"class": "logging.handlers.RotatingFileHandler",
				"filename": "./logs/replication.log",
				"maxBytes": 10000000,
				"backupCount": 200,
				"encoding": "utf8"
			}
		}


	}