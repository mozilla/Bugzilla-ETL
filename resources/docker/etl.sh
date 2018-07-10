#!/usr/bin/env bash
cd $HOME
export PYTHONPATH=.:vendor
python ./bzETL/bz_etl.py --settings=resources/docker/config.json
