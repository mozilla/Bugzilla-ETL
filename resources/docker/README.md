# Docker

## Instructions

All commands are meant to be run from the root directory for this repo; not this directory, rather its grandparent.

### Build

```bash
docker build \
       --file resources\docker\etl.dockerfile \
       --no-cache \
       --tag test-etl \
       .
```

*This command is also in the `build.sh` script*


### Configuration

The `config.json` file is the single source for all parameters required to run Bugzilla-ETL. It is properly configured for running both public and private ETL. Notice it contains references to environment variables (eg `{"$ref":"env://LOG_APPNAME"}`) and those variables are defined in the `*.env` files as examples. It is expected you will `docker run` with `-e` for each of those variables you want to override, or provide your own environment file with the secrets set.

### Run

Once the docker image is built, you may run it:

```bash
docker run \
       --user app \
       --env-file ./resources/docker/public_etl.env \
       --mount source=public_etl_state,destination=/app/logs \
       test-etl 
```

This will not work until you update the environment file (`public_etl.env`) with suitable values.

**Notes**

* The environment variables file (`public_etl.env`) lists all parameters you must set: Pointers to servers it touches, and values for the secrets.
* The docker image requires inter-run state; both for logs and the current ETL status; be sure to mount some (small) amount of storage to `/app/logs`.  You can change this in the `config.json` file
