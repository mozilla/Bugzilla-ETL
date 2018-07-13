#!/usr/bin/env bash

# RUN FROM ROOT Bugzilla-ETL DIRECTORY, eg ./resources/docker/build.sh
docker run --interactive --tty --user app --env-file ./resources/docker/private_etl_dev.env --mount source=public_etl_state,destination=/app/logs test-etl bash
