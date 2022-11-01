#Etl cruds

###Postgres setup

**cached build:**

`docker-compose up --build -d`

**No cache build :** 


`docker-compose rm -f && docker-compose pull && docker-compose up --build -d `



###In case all the posrgres ports are busy run this: 

**show all occupied ports:**

`netstat -anvp tcp | awk 'NR<3 || /LISTEN/'`

**kill process that holds the port:**

`sudo kill <pid>`

**In case you see this error:** 

_PostgreSQL Database directory appears to contain a database; Skipping initialization_

**run this:**

`docker-compose down --volumes`


**Inserting data into database Insert Performance Benchmark:**

https://naysan.ca/2020/05/09/pandas-to-postgresql-using-psycopg2-bulk-insert-performance-benchmark/
https://innerjoin.bit.io/populating-a-postgresql-table-with-pandas-is-slow-7bc63e9c88dc
