
Mozilla's Bugzilla instance stores time in local DST-adjusted Pacific Time.
The ETL script converts all times to GMT for ease of comparison.  If you plan to
run tests or generate your own ES instance from a MySQL database
you will need to install the timezone database to perform this conversion.

Windows

  * Shutdown MySQL service
  * Copy contents of zip file to ```mysql``` schema direcotry (C:\ProgramData\MySQL\MySQL Server 5.5\data\mysql on Windows)
  * Start MySQL service

Linux ([more instructions](http://dev.mysql.com/doc/refman/4.1/en/mysql-tzinfo-to-sql.html))

  * Unzip timezone_2011n_posix.zip to directory
  * ```mysql_tzinfo_to_sql <directory> | mysql -u root mysql```



