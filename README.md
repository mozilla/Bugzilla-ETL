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
argument in the command line (see [example command line script](https://github.com/klahnakoski/Bugzilla-ETL/blob/master/resources/scripts/bz_etl.bat))

Here is my ```settings.json``` file:

    {
		"param":{
			"start":0,
			"increment":1000,
			"alias_file":"./resources/data/bugzilla_aliases.txt"
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
			"schema_file":"./resources/json/bug_version.json"
		},
		"debug":{
        	"log":[{
    			"class": "logging.handlers.RotatingFileHandler",
    			"filename": "./resources/logs/replication.log",
    			"maxBytes": 10000000,
    			"backupCount": 200,
    			"encoding": "utf8"
    		},{
                "class":"util.debug.Log_usingStream",
                "stream":"sys.stdout"
            }]
		}


	}))

