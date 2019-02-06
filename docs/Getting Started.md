
# Getting Started with bzETL

This document should be expanded 

* The public bugs are exposed at this endpoint: https://activedata-public.devsvcprod.mozaws.net/query
* The endpoint accepts "json query expresssions": https://github.com/mozilla/ActiveData/blob/dev/docs/jx.md
* An example page that uses that endpoint: https://s3-us-west-2.amazonaws.com/charts.mozilla.org/metrics/Tutorial01-Minimum.html
* Each document is a "snapshot": The stat of the bug between `modified_ts` and `expires_on` timestamps.  Timestamps are in **milliseconds since epoch**.  The current bug snapshot can be found with `{"eq":{"expires_on":9999999999000}}`
* There is a primitive query tool (static site) that can be used to POST queries ot the endpoint, and see the result: https://activedata-public.devsvcprod.mozaws.net/tools/query.html#query_id=byI46JQd
* Another getting started guide:  Although it does not refer to the schema found in this project, the strategies are: https://github.com/mozilla/ActiveData/blob/dev/docs/GettingStarted.md


I suggest you tell me what you want, I will respond with a query. After a few questions you will get a sense of the schema.

