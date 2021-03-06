# 2018 Q1 Overview and Upgrade Plan

What follows is the general high-level architecture of all the important parts, how it will be changing in the context of the Bugzilla cluster, and where help would be appreciated.

## Overview 

### Old Architecture

This has been around for a while now. The "Toronto Server" connections no longer exist.

![](Architecture_2015.png)

### New Architecture

This is the plan for 2018. Overall, it is correct, but the details must be refined as the details are understood.

#### [Bug 1429487](https://bugzilla.mozilla.org/showdependencytree.cgi?id=1429487&hide_resolved=1) - [meta] Bugzilla-ETL upgrade to ESv6 

![](Architecture_2018.png)


### Public and Private Clusters [[wiki](https://wiki.mozilla.org/BMO/ElasticSearch)]

ElasticSearch is being used as a data warehouse, and is storing bug snapshots for every bug, every change, every annotation, and every comment. As an example, [here are the snapshots for one bug](https://charts.mozilla.org/metrics/Tutorial01-Minimum.html). Each snapshot is a JSON document representing the state of the bug between the `modified_ts` and `expires_on` timestamps.

This cluster is very fast. You can get information on tens of thousands of bugs from a single request, and in seconds. Even if you do not drive dashboards off of it directly, you can use it as a datasource; it is much faster to get the bug data you need from ES than from Bugzilla. 

#### Action Required (mostly OPS)

1. Setup ES6 cluster (or service) - no data migration is required since the ETL transformation code was designed to handle ES instability, and it will autofill missing bugs (at about 200 kilobugs per hour, newest bugs first).
2. Teardown old cluster when done


### ETL Pipeline [[code](https://github.com/klahnakoski/Bugzilla-ETL)] [[bugs](https://bugzilla.mozilla.org/showdependencytree.cgi?id=959670&hide_resolved=1)]

The ETL pipeline is scheduled to run every 10 minutes, scan the Bugzilla database for all changes since last scan, and update the cluster. The code has some known bugs (listed [on Bugzilla](https://bugzilla.mozilla.org/showdependencytree.cgi?id=959670&hide_resolved=1) not on Github), but they have not been a priority for years. 

#### Action Required (in Python)

1. Fix inconsistency bug - The ETL sometimes corrupts the snapshots due to a parallelism bug: [https://bugzilla.mozilla.org/show_bug.cgi?id=1063125](https://bugzilla.mozilla.org/show_bug.cgi?id=1063125) making the snapshots inconsistent (overlapping time intervals). This turns out to not be a big issue as the corruption is fixed the next time the bug is changed, leaving very few inconsistent snapshots. 
  * This may already be written, but not tested, in the `v2` branch
2. Add the Typed Encoder - The new version of Elasticsearch is more like a pedantic database than a flexible data lake; we must use the [Typed Encoder](https://github.com/klahnakoski/mo-json/blob/master/mo_json/typed_encoder.py) to transform the JSON documents. The format allows us to automate the schema management, as Bugzilla schema inevitably changes over the years.
  * Jan8 - Looks like the typed encoder must be enhanced to handle [Elasticsearch multifields](https://www.elastic.co/guide/en/elasticsearch/reference/current/multi-fields.html)
3. Optional - Use Pulse - Pulse emits a list of all bug numbers that have changed; rather than running the ETL in batch mode, it can run continuously and be only a ?couple? seconds behind.
4. Optional - Upgrade the `pyLibrary` - This is copy of my old pyLibrary, which has split into multiple packages, and made to work with Python3. The ETL code should be upgraded to use these new libs; which must also be vendorded into the repo so there are no pip dependencies (except maybe `requests`). 
5. Optional - Upgrade to Python 3 - Since this program will not be touched again in many years, it may be a good time to get it to work on Python3.


Deploying will take time because the production code runs on servers we can not access. OPS will be involved as inevitable production bugs appear.

#### Why help?

Bugzilla-ETL is interesting for the fact it is responsible for tracking [slowing-changing-dimensions](https://en.wikipedia.org/wiki/Slowly_changing_dimension) and it may be the only ETL code at Mozilla responsible for tracking a complex lifecycle. Understanding this code will reveal an opportunity to abstract the management of these slowing-changing-dimensions so it can be used in other ETL pipelines. This project also has a good test suite that you can use to verify your implementation is correct.

This code was derived from a flow-based programming environment, and has plenty of stream context variables that are not needed in a batch processing case. By changing the architecture to run in match mode, we will be able to breakdown the ETL to finer steps, and maybe run on AWS lambda.


### esFrontline [[code](https://github.com/klahnakoski/esFrontLine)]

esFrontline is a very basic proxy server for the public cluster that limits http requests to just query requests. This will be replaced by an [ActiveData](https://github.com/klahnakoski/ActiveData) instance.

#### Action Required (mostly OPS)

* Remove 


### ActiveData [[code](https://github.com/klahnakoski/ActiveData)] [[wiki](https://wiki.mozilla.org/EngineeringProductivity/Projects/ActiveData)]

ActiveData is near-stateless web service that wraps an ES cluster to make it behave as a simple data warehouse. Most importantly, it translates [JSON Query Expressions](https://github.com/klahnakoski/ActiveData/blob/dev/docs/jx.md) into ES queries.

An ActiveData instance will replace the esFrontLine in front of the public cluster. Another instance will be put in front of the private cluster, behind the VPN.  

#### Action Required (mostly OPS)

1. Setup front ends for the private and public clusters.  
2. Use Nginx so we can plugin LDAP access controls: See [https://github.com/mozilla-iam/mozilla.oidc.accessproxy](https://github.com/mozilla-iam/mozilla.oidc.accessproxy)

### Post-ETL Server [[code](https://github.com/klahnakoski/MoDevETL)]

The calculations made for review queues and bug hierarchy lookup is done with a post-etl cron job. This does not change the data in the clusters, rather it files new tables with derived review queue and hierarchical information. 

#### Action Required

1. Merge code with Bugzilla-ETL

#### Why Help?

This code inspects the bug life cycle to extract reviews. Reviews can get complicated; review flags can be simply deleted (r-); multiple reviewers can be assigned, with only one giving a review; Reviews can be handed off to other users; etc. In each case we want to know what it means to "complete a review".  Not all the work is done in this field, and it would be nice to handle the complicated cases.

Not as interesting, but: The hierarchy aggregator is responsible for using the `blocked_by` field to find all ancestors and predecessors of a bug; not only for a given point in time, but for all time of the bug's life. These long lists can serve as a short-list to consider for dependency trees over time. There are MoDevMetrics charts that tracked large dependency trees over time. They were good for identifying when blockers were added that had deep dependency lists.


### MoDevMetrics [[code](https://github.com/klahnakoski/MoDevMetrics)]

MoDevMetrics is a collection of javascript libraries and helper methods to write dashboards using the ES clusters directly. With this collection, dashboards are usually one page of HTML, queries, and javascript. Queries are in a special format called [JSON Query Expressions](https://github.com/klahnakoski/ActiveData/blob/dev/docs/jx.md), which are succinctly described as "SQL parse trees in JSON".

The MoDevMetrics has a few main parts

* **The ES query translator** - ES does not have a SQL interface, it was not designed for data warehouse queries. As such, all queries are translated to ElasticSearch queries. This translation is quite complex since ESv0.9 had limited query ability.
* **Internal Query Runner** - Accepts queries and operates on data locally. Since I come from a database background, I find expressing data transformation as queries more clear than using javascript. 
* **Charting libraries** - Since every charting library has some lethal deficiency, I made a common API to a couple charting of libraries. 
* **Dimension Definitions** - Maps business concepts like "open bug" or "current bug state" into filters required to extract that, which at times gets complicated. 

#### Action Required (in Javascript)

1. Remove the javascript ES query translator code so that queries are sent directly to ActiveData, and existing dashboards still work with the new architecture. This can only be done after we have a prototype for the new Bugzilla-ETL, and a (dev) cluster exists to test with.
2. Work with `armenzg` and `wlach` to identify any common ground when it comes to charting libraries and  resources in the dashboarding space.
3. Optional - Add testing to the internal query runner. There are currently no tests for this code, but there is a comprehensive Python test suite. There should be a simple solution here.
4. Optional - Use the React/JSX stack. A good part of MoDevMetrics can be removed if it gets moved to a React app. plus, it will make everything faster.

#### Why Help?

Charting and dashboarding tools have 5 major components. I listed each component, along with what MoDevMetrics uses:

* charts (various chart libs)
* dashboard layout (html/css)
* business rules (JSON documents holding rules and business data)
* query language and analysis tools (JSON Query Expressions)
* datasources (Elasticsearch, local js objects)

Business Intelligence (BI) tools tightly integrate these 5 components to provide a high-value service to business customers. The value is so high that the major open source BI vendors are quickly bought by established competitors to reduce competition. The complexity of the un-maintained open source product is too much for the community to support, and the product dies.

I believe open source software can make the dashboarding space cheaper and better by ensuring the components communicate via *standards*. With standards, development can proceed in parallel, the smaller components are easier for the community to support, with multiple contenders for each.  

MoDevMetrics is not close to this panacea, but it does implement some complex dashboards that will help you experience the complexity of this space: You can help define and clarify the standards that should separate these components.   


### Metrics Graphics [[code](https://github.com/mozilla/metrics-graphics)] [[docs](https://www.metricsgraphicsjs.org/)]

This is one of the charting libraries used by MoDevMetrics.  A `armenzg` is interested in dashboarding and has expressed interest in upgrading it so it can be easily used in JSX/React apps.   

#### Why Help?

This library could use help. If you are interested in graphics, svg and web assembly then this project will give you that experience while giving happiness to people viewing your work.
 

