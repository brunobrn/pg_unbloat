# pg_unbloat

PG_Unbloat is a automation to identify bloated tables in a single database.

We have a great extension on PostgreSQL to help us to identify bloating in PostgreSQL and this automation even uses it, but in certain tables even this extension don't works well.

Here the extension pg_stattuple help us to filter high bloated tables from non bloated tables, using the extension with pg_stattuple_approx first and then using pg_stattuple to do a full SEQ SCAN in the table to see the percentage of bloat. 

After that the automation literally copy only the data from the original table and we compare the size of the two tables, the original and the copied table, only useful data are copied and from that we have the correct value of bloating. We dont considere the size of indexes on this calculations (We have to consider the metadata in the pages too, but the metadata it is very light )


# Usage

The usage is simple, clone this repo or use the docker image.

The password always be asked in every execuction.

``` sh
python exec_unbloat.py --host foo-bar.mycloud.com -U postgres -d foo-bar-database
```

<b>Parameters:</b> Type this for help 

```sh
$ python exec_unbloat.py -h 
```
```
bruno | ~/Documents/dlpco/pg_unbloat/pg_unbloat | main ± | 19:48:18 $ python exec_unbloat.py -h 

usage: exec_unbloat.py [-h] --host HOST --dbname DBNAME --user USER [--port PORT] [--table_min_size TABLE_MIN_SIZE] [--table_max_size TABLE_MAX_SIZE]

This automation identify bloated tables in a especific database, it's not possible run this automation in a read replica and the user needs to have superuser grants

options:
  -h, --help            show this help message and exit
  --host HOST, --HOST HOST
                        Host address to access the database. Ex: 127.0.0.1
  --dbname DBNAME, --DBNAME DBNAME, -d DBNAME
                        Name of the database to check tables, yes we can execute only in one database at a time
  --user USER, --USER USER, -U USER
                        Name of user to connect in the database, the user need to have superuser grants
  --port PORT, --PORT PORT, -p PORT
                        Port of to connect on instance
  --table_min_size TABLE_MIN_SIZE, --TABLE_MIN_SIZE TABLE_MIN_SIZE
                        Minimum size to scan tables
  --table_max_size TABLE_MAX_SIZE, --TABLE_MAX_SIZE TABLE_MAX_SIZE
                        Maximum size to scan tables, when bigger the value more time and I/O the automation will use.


```