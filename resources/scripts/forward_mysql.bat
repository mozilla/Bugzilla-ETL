REM -N DO NOT START SHELL
REM -v VERBOSE
REM -L <local_port>:<distant_host>:<distant_port> <putty config>

plink -v -N -L 3307:bugzilla-masterdb-devsvcdev-2017100901.czvlmp16hwe4.us-west-2.rds.amazonaws.com:3306 CloudOps-ETL