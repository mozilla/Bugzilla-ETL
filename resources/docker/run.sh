#!/usr/bin/env bash

# RUN FROM ROOT Bugzilla-ETL DIRECTORY, eg ./resources/docker/build.sh
docker run --interactive --tty --user app --env-file ./resources/docker/public_etl.env --mount source=public_etl_state,destination=/app/logs test-etl bash



docker run --interactive --tty --user app --env-file ./resources/docker/dev_private_etl.env --mount source=public_etl_state,destination=/app/logs test-etl bash
