#!/usr/bin/env bash


# RUN FROM ROOT Bugzilla-ETL DIRECTORY
docker build --file resources\docker\etl.dockerfile --no-cache --tag test-etl .

