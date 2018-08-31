#!/usr/bin/env bash


# RUN FROM ROOT Bugzilla-ETL DIRECTORY
docker build --file resources\docker\etl.dockerfile --build-arg REPO_CHECKOUT=v2 --build-arg BUILD_URL=%CIRCLE_BUILD_URL% --no-cache --tag bugzilla-etl .

