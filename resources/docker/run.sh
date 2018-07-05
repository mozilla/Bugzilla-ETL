#!/usr/bin/env bash

# RUN FROM ROOT Bugzilla-ETL DIRECTORY
docker run \
       --interactive \
       --tty \
       --user app \
       --env-file ./resources/docker/public_etl.env \
       --mount source=public_etl_state,destination=/app/logs \
       test-etl \
       bash
