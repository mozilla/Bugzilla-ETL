
Setup Replication
=================

I strongly suggest setting up your own ES cluster, and replicating Mozilla's public cluster.  The benefits are:

  * **Faster query response** - Your queries need not compete with others, so your query results will be cached for longer.  Also, ES is very low latency: a cluster on your own network will have better response time.
  * **More query flexibility** - MVEL scripting has been disabled on the public cluster.  Having your own default installation of ElasticSearch will open up many more query features.



Requirements
-----------

Installation
------------

  * Download ElasticSearch
  * Install ES
  * Run replication

