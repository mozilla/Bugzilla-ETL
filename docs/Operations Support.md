

## Operations Support for Bugzilla-ETL

Mozilla runs an instance of this software to fill an ES cluster with bug snapshots. If there is a problem, you may want to investigate:

- **[Information from an OPS perspective](https://mana.mozilla.org/wiki/pages/viewpage.action?pageId=83788855)** - all the pointers to machines
- **[Accessing Logs](https://mana.mozilla.org/wiki/display/SVCOPS/Accessing+the+logging+bastions+and+rawlogs+hosts)** - URLs for getting to the logging servers, including sample `ssh` commands, and logging directory.

Also, there is an ActiveData instance that serves off the ES cluster

- **[ActiveData Code](https://github.com/mozilla/ActiveData)**
 
### Viewing Logs

The logs are structured logs, so you will need `jq` to format them nicely:

```bash
    tail -f bzetl.etlhost.file.public_bzetl_log.log | jq -R fromjson?
```

The `-R fromjson?` is required to filter out the non-json also found in the logs. These non-json lines are during the short period of time at startup (and shutdown) when the program is generating logs, but the structured logging module has not started.

### New Bugs

If there is a problem, open a bug in Buzilla: [`Cloud Services::Operations: Bzetl`](https://bugzilla.mozilla.org/enter_bug.cgi?product=Cloud%20Services&component=Operations%3A%20Bzetl)


