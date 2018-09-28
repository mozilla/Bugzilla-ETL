

## Caring for Bugzilla-ETL

This project fills an ES cluster with bug snapshots. If there is a problem, you may want to investigate:

- **Information from an OPS perspective** - [https://mana.mozilla.org/wiki/pages/viewpage.action?pageId=83788855](https://mana.mozilla.org/wiki/pages/viewpage.action?pageId=83788855)
- **Accessing Logs** - [https://mana.mozilla.org/wiki/display/SVCOPS/Accessing+the+logging+bastions+and+rawlogs+hosts](https://mana.mozilla.org/wiki/display/SVCOPS/Accessing+the+logging+bastions+and+rawlogs+hosts)

There is an ActiveData instance that serves off the cluster

- **ActiveData Code** - [https://github.com/mozilla/ActiveData](https://github.com/mozilla/ActiveData)
 
### Viewing Logs

The logs are structured logs, so you will need `jq` to format them nicely:

```bash
    tail -f bzetl.etlhost.file.public_bzetl_log.log | jq -R fromjson?
```

The `-R fromjson?` is required to filter out the non-json also found in the logs. These non-json lines are during the short period of time at startup (and shutdown) when the program is generating logs, but the structured logging module has not started.
