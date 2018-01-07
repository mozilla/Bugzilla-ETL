My previous mail had some details of the existing security charts using the ES (version 0.9) cluster.  This quarter I should be working on upgrading that cluster, and the peripheral dashboards that use it.




# 2018 Q1 Overview and Upgrade Plan

What follows is the general high-level architecture of all the important parts, how it will be changing in the context of the Bugzilla cluster, and where help would be appreciated.
MAIN PARTS


## Overview 

[![](Architecture.png)

### Public and Private Clusters [[wiki](https://wiki.mozilla.org/BMO/ElasticSearch)]

ElasticSearch is being used as a data warehouse, and is storing bug snapshots for every bug, every change, every annotation, and every comment. As an example, [here are the snapshots for one bug](https://charts.mozilla.org/metrics/Tutorial01-Minimum.html). Each snapshot is a JSON document representing the state of the bug between the `modified_ts` and `expires_on` timestamps.

This cluster is very fast. You can get information on tens of thousands of bugs from a single request, and in seconds. Even if you do not drive dashboards off of it directly, you can use it as a datasource; it is much faster to get the bug data you need from ES than from Bugzilla. 

These are at version 0.9 and must be upgraded. This is probably the most trivial action since the ETL transformation code was designed to handle ES instability, and autofill missing bugs (at about 200K per hour, newest bugs first).

### ETL Pipeline [[code](https://github.com/klahnakoski/Bugzilla-ETL)]

The ETL pipeline is scheduled to run every 10 minutes, scan for all changes since that time, and update the cluster. The code has some known bugs (listed [on Bugzilla](https://bugzilla.mozilla.org/showdependencytree.cgi?id=959670&hide_resolved=1) not on Github), but they have not been a priority for years. There are two main items to complete for Q1: 

1. The ETL corrupts the snapshots due to a parallelism bug: [https://bugzilla.mozilla.org/show_bug.cgi?id=1063125](https://bugzilla.mozilla.org/show_bug.cgi?id=1063125) This turns out to not be a big issue as the corruption is fixed the next time the bug is changed, leaving very few inconsistent snapshots.
2. The new versions of Elasticsearch (after version 1.x) are more like a pedantic database than a flexible data lake; we must use the [Typed Encoder](https://github.com/klahnakoski/mo-json/blob/master/mo_json/typed_encoder.py) to transform the JSON documents. The format allows us to automate the schema management so we need not to, as Bugzilla inevitably changes over the years.


### esFrontline [[code](https://github.com/klahnakoski/esFrontLine)]

esFrontline is a very basic proxy server for the public cluster that limits http requests to just query requests. This will be replaced by an [ActiveData](https://github.com/klahnakoski/ActiveData) instance.


### MoDevMetrics [[code](https://github.com/klahnakoski/MoDevMetrics)]

MoDevMetrics is a collection of javascript libraries and helper methods to write dashboards using the ES clusters directly. With this collection, dashboards are usually one page of HTML, queries, and javascript. Queries are in a special format called [JSON Query Expressions](https://github.com/klahnakoski/ActiveData/blob/dev/docs/jx.md), which are best described as SQL parse trees in JSON.

The MoDevMetrics has a few main parts

1. **The ES query translator** - ES does not have a SQL interface, it was not designed for data warehouse queries. As such, all queries are translated to ElasticSearch queries. This translation is quite complex since ESv0.9 had limited query ability. This part can be ripped out, since ActiveData can accept these queries directly. 
2. **Internal Query Runner** - Accepts queries and operates on data locally. Since I come from a database background, I find expressing data transformation as queries more clear than using javascript. 
3. **Charting libraries** - Since every charting library has some lethal deficiency, I made a common API to a couple charting libraries. 
3. **Dimension Definitions** - Maps natural concepts like "open bug" or "current bug state" into filters required to extract that, which at times gets complicated. 


## Other parts


### ActiveData [[code](https://github.com/klahnakoski/ActiveData)] [[wiki](https://wiki.mozilla.org/EngineeringProductivity/Projects/ActiveData)]

ActiveData is near-stateless web service that wraps an ES cluster to make it behave as a simple data warehouse. Most importantly, it translates [JSON Query Expressions](https://github.com/klahnakoski/ActiveData/blob/dev/docs/jx.md) into ES queries. 

### Metrics Graphics [[code](https://github.com/mozilla/metrics-graphics)] [[docs](https://www.metricsgraphicsjs.org/)]

This is one of the charting libraries used by MoDevMetrics.  A person n my team is interested in dashboarding and has expressed interest in upgrading it so it can be easily used in JSX/React apps.   



 



 

