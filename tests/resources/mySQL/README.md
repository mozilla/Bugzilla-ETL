
Mozilla's Bugzilla instance stores time in local DST-adjusted Pacific Time.
The ETL script converts all times to GMT for ease of comparison.  If you plan to
run tests or generate your own ES instance from a MySQL database
you will need to install the timezone database to perform this conversion.

Windows
-------

  * Shutdown MySQL service
  * Copy contents of zip file to ```mysql``` schema directory (```C:\ProgramData\MySQL\MySQL Server 5.5\data\mysql``` on Windows)
  * Start MySQL service

Linux with zoneinfo database
----------------------------

  * Do you have a ```/usr/share/zoneinfo``` directory (or equivelant)?
  * ```mysql_tzinfo_to_sql <directory> | mysql -u root mysql```

From [http://dev.mysql.com/doc/refman/5.0/en/time-zone-support.html](http://dev.mysql.com/doc/refman/5.0/en/time-zone-support.html):

  > If your system has its own zoneinfo database (the set of files describing time zones), you should use the ```mysql_tzinfo_to_sql``` program for filling the time zone tables. Examples of such systems are Linux, FreeBSD, Solaris, and Mac OS X. One likely location for these files is the ```/usr/share/zoneinfo``` directory.

Linux without zoneinfo database
-------------------------------

  * Unzip timezone_2011n_posix.zip to a directory
  * ```mysql_tzinfo_to_sql <directory> | mysql -u root mysql```


([more Linux instructions](http://dev.mysql.com/doc/refman/4.1/en/mysql-tzinfo-to-sql.html))