# mongodb-cloudsearch

Real Time Integration MongoDB to Amazon Cloudsearch.

**mdbcs.cnf** <br>
Configuration file

```
[DEFAULT]
file_last_ts_default = last_timestamp/last_ts_default.txt #default txt file to save last timestamp
bulk_insert_amount = 1 #bulk insert amount data
bulk_update_amount = 5000 #bulk update amount data

#MongoDB
mongodb_host_primary = mongo10.net.br:27017 #mongodb host
mongodb_host_secondary = mongo20.net.br:27017 #mongodb host
mongodb_database = database_live #database
mongodb_database_log = integracao #database log

#Amazon CloudSearch
cloudsearch_user = automate #user cloudserach
cloudsearch_id = CLOUDSERACHID #cloudserach id
cloudsearch_key = Cl0udS34rchK3y #cloudsearch key
cloudsearch_domain = search-live #domain
cloudsearch_region = us-east-1 #aws region

[MONGODB COLLECTION]
collection = collection 
oplog_ns = %(mongodb_database)s.%(collection)s
file_last_ts_all = last_timestamp/last_ts_%(collection)s_all.txt
file_last_ts_insert = last_timestamp/last_ts_%(collection)s_insert.txt
file_last_ts_update = last_timestamp/last_ts_%(collection)s_update.txt
file_last_ts_delete = last_timestamp/last_ts_%(collection)s_delete.txt
```

**mongodb_cloudsearch.py** <br>
Main()

Example: 
> python mongodb_cloudsearch.py -c drivers -o insert -d 1

Options:
```
  -h, --help            show this help message and exit
  -c COLLECTION, --collection=COLLECTION
                        Reads the specified collection from MongoDB Oplog
  -o OPERATION, --operation=OPERATION
                        Reads the specified CRUD operations
                        [insert], [update], [delete], [all]
  -d DEBUG, --debug=DEBUG
                        Show Debug [bool]
```

**fields_format.py**<br>
Format Class fields from MongoDB to CloudSerach

