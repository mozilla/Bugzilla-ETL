REM -N DO NOT START SHELL
REM -v VERBOSE
REM -L <local_port>:<distant_host>:<distant_port> <putty config>

plink -v -N -L 9200:vpc-vpc-etl-es-public-devsvcdev-debriixex5z7p3orlxtsbkvhoi.us-west-2.es.amazonaws.com:443 CloudOps-ETL