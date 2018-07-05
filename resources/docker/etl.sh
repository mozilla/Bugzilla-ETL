#!/usr/bin/env bash
cd /app
SET PYTHONPATH=.:/vendor
python ./bzETL/bz_etl.py --settings=resources/docker/config.json
