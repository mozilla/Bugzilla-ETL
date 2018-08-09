#!/usr/bin/env bash

# RUN FROM ROOT Bugzilla-ETL DIRECTORY, eg ./resources/docker/build.sh
docker run --user app --env-file ./resources/docker/public_etl.env --mount source=public_etl_state,destination=/app/logs bugzilla-etl bash
