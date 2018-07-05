#!/usr/bin/env bash
docker build \
       --file resources\docker\etl.dockerfile \
       --tag test-etl \
       .

