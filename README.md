## Spark exercises solving

### Description
Use Scala for all spark connected code. It would be great to implement solution both in terms of RDDs and Datasets.
* 5.1 Select top 10  most frequently purchased categories
* 5.2 Select top 10 most frequently purchased product in each category
* 6. JOIN events with geodata
* 6.1 Usage of https://www.maxmind.com/en/geoip2-country-database is prohibited
* 6.2 Put data from http://dev.maxmind.com/geoip/geoip2/geolite2/ to HIVE table
* 6.3 JOIN events data with ip geo data
* 6.4 Select top 10 countries with the highest money spending
Put queries result from step 5 and 6 result to RDBMS


### How to run
fill out application.conf and do "sbt -Dspark.master=local[*] run" to run in place or specify path to your config file
sbt assembly to package jar for submitting by spark-submit
