#Dask crud

#Setup

**cached build:**

`docker-compose up --build -d`

**No cache build :** 


`docker-compose rm -f && docker-compose pull && docker-compose up --build -d `



###In case all the posrgres ports are busy run this: 

**show all occupied ports:**

`netstat -anvp tcp | awk 'NR<3 || /LISTEN/'`

**kill process that holds the port:**

`sudo kill <pid>`

###In case you see this error: _PostgreSQL Database directory appears to contain a database; Skipping initialization_ , run this:

`docker-compose down --volumes`
