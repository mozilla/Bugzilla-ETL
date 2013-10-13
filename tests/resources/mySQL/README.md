
If you plan to run tests, you must install the timezone database:

Windows

  * Shutdown MySQL service
  * Copy contents of zip file to ```mysql``` schema direcotry (C:\ProgramData\MySQL\MySQL Server 5.5\data\mysql on Windows)
  * Start MySQL service

Linux ([more instructions](http://dev.mysql.com/doc/refman/4.1/en/mysql-tzinfo-to-sql.html))

  * Unzip timezone_2011n_posix.zip to directory
  * ```mysql_tzinfo_to_sql <directory> | mysql -u root mysql```



