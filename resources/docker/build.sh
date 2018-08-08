#!/usr/bin/env bash


# RUN FROM ROOT Bugzilla-ETL DIRECTORY
docker build --file resources\docker\etl.dockerfile --build-arg REPO_CHECKOUT=v2 --no-cache --tag bugzilla-etl .

