#!/usr/bin/env bash

# RUN FROM ROOT Bugzilla-ETL DIRECTORY, eg ./resources/docker/debug.sh
docker run -it --dns 8.8.8.8 --env-file ../../bugzilla-etl-debug.env --mount source=public_etl_state,destination=/app/logs bugzilla-etl bash



docker run --user app --env-file ../../bugzilla-etl-debug.env --mount source=public_etl_state,destination=/app/logs bugzilla-etl
