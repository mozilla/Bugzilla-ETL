#!/usr/bin/env bash
cd /app
export PYTHONPATH=.:/vendor
python ./bzETL/bz_etl.py --settings=resources/docker/config.json
