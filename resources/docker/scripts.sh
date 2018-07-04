#!/usr/bin/env bash

# find image to start with

# login and record commands

docker run --interactive --tty python:3.6.1 bash

# deliver env vars to image
docker run -it --user app --env-file ./resources/docker/public_etl.env test-etl bash


# COPY FILES INTO IMAGE
docker cp . 9fb5624531ae:/app

# SAVE DOCKER PROCESS TO IMAGE
docker commit cf26c8d0d44 test-etl

# make a dockerfile with those commands

# setup cron
https://jonathas.com/scheduling-tasks-with-cron-on-docker/

# save image for later
https://stackoverflow.com/questions/19585028/i-lose-my-data-when-the-container-exits#19616598


# USE ubuntu IMAGE SO THERE ARE TOOLS

# ADD CRON ENTRY
https://www.ekito.fr/people/run-a-cron-job-with-docker/

# ADD VOLUME TO STORE STATE
