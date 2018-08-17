#!/usr/bin/env bash

# find image to start with

# login and record commands

docker run --interactive --tty ubuntu bash

# deliver env vars to image
docker run -it --user app --env-file ./resources/docker/public_etl.env --mount source=public_etl_state,destination=/app/logs test-etl bash




# COPY FILES INTO IMAGE
docker cp . 9fb5624531ae:/app

# SAVE DOCKER PROCESS TO IMAGE
docker commit cf26c8d0d44 test-etl

# make a dockerfile with those commands

# setup cron
https://jonathas.com/scheduling-tasks-with-cron-on-docker/

# save image for later
https://stackoverflow.com/questions/19585028/i-lose-my-data-when-the-container-exits#19616598


# ADD VOLUME TO STORE STATE



cd ~/Bugzilla-ETL  # ENSURE YOU ARE IN THE ROOT OF THE Bugzilla-ETL REPO
docker build --file resources\docker\etl.dockerfile --no-cache --tag test-etl .





docker run --interactive --tty --user app --env-file ../../dev_private_etl.env --mount source=public_etl_state,destination=/app/logs test-etl bash
docker run --user app --env-file ../../dev_private_etl.env --mount source=public_etl_state,destination=/app/logs test-etl

python bugzilla_etl/bz_etl.py --settings=resources/docker/config.json



# CLEANUP

https://www.calazan.com/docker-cleanup-commands/
