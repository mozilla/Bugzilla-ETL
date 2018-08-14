REM -N DO NOT START SHELL
REM -v VERBOSE
REM -L <local_port>:<distant_host>:<distant_port> <putty config>

plink -v -N -L 9201:vpc-vpc-etl-es-public-devsvcdev-debriixex5z7p3orlxtsbkvhoi.us-west-2.es.amazonaws.com:443 CloudOps-ETL


plink -v -N -L 9201:vpc-vpc-etl-es-private-devsvcdev-gxmvb6nmr54p7iadjthd2e6usa.us-west-2.es.amazonaws.com:443 CloudOps-ETL


