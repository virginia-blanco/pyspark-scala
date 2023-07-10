from pyspark.sql import SparkSession 
from pyspark.sql.types import *
import pyspark.sql.functions as f
import sys
from pyspark.sql import SparkSession

if __name__ == "_main_":
    if len(sys.argv) != 3:
        print("Usage : file error", sys.stderr)
        sys.exit(-1)

spark = (SparkSession
    .builder
    .appName("week-10")
    .enableHiveSupport()
    .getOrCreate())

delaysPath = sys.argv[1]
print("****************************************************",delaysPath,"****************************************************")
airportsPath = sys.argv[2]
print("****************************************************",airportsPath,"****************************************************")

airports = (spark.read
  .format("csv")
  .options(header="true", inferSchema="true", sep="\t")
  .load(airportsPath))

# airports = (spark.read.options(header="true", inferSchema="true", sep="\t").load(airportsPath))

airports.createOrReplaceTempView("airports")

#CREATE DATABASE firstname-lastname-week-10 IF NOT EXIST

delays = (spark.read
  .format("csv")
  .options(header="true")
  .load(delaysPath))

delays = (delays
  .withColumn("delay", f.expr("CAST(delay as INT) as delay"))
  .withColumn("distance", f.expr("CAST(distance as INT) as distance")))

delays.createOrReplaceTempView("delays")

# Create temporary small table
foo = (delays
  .filter(f.expr("""origin == 'SEA' and destination == 'SFO' and 
    date like '01010%' and delay > 0""")))
foo.createOrReplaceTempView("foo")

# spark.sql("SELECT * FROM airports LIMIT 10").show()
# spark.sql("SELECT * FROM delays LIMIT 10").show()
# spark.sql("SELECT * FROM foo LIMIT 10").show()

#delays.join(airports, delays("origin")==airports("IATA"),"inner").join(airports, delays("destination")==airports("IATA"),"inner").show()
aux_DF = delays.join(airports, delays.origin==airports.IATA,"inner")
aux_DF = (aux_DF
  .withColumn("origin city", f.col("City"))
  .withColumn("origin state", f.col("State"))
  .withColumn("origin coutry", f.col("Country")))
# a.drop(f.col("City"), f.col("State"), f.col("Country"))
aux_DF = aux_DF.drop("City", "State", "Country", "IATA")
aux_DF.show()
aux_DF = aux_DF.join(airports, aux_DF.destination==airports.IATA,"inner")
aux_DF = (aux_DF
  .withColumn("destination city", f.col("City"))
  .withColumn("destination state", f.col("State"))
  .withColumn("destination coutry", f.col("Country")))
aux_DF = aux_DF.drop("City", "State", "Country", "IATA")
aux_DF.show()

foo_DF = aux_DF.filter((f.col("origin")=="SEA") & (f.col("destination")=="SFO") & (aux_DF.date.like('01010%')) & (f.col("delay")>0))
foo_DF.show()

# aux_DF.createOrReplaceTempView("joinTable")


# create database a if it doesn't exist and switch to it
spark.sql("CREATE DATABASE IF NOT EXISTS virginiablancoravenaweek10")
spark.sql("USE virginiablancoravenaweek10")

# spark.sql("CREATE TABLE IF NOT EXISTS virginiablancoravenaweek10 AS SELECT * FROM joinTable")
# spark.sql("SELECT * FROM virginiablancoravenaweek10").show()

foo_DF.createOrReplaceTempView("joinTableFilter")

spark.sql("CREATE TABLE IF NOT EXISTS virginiablancoravenaweek10filter AS SELECT * FROM joinTableFilter WHERE origin == 'SEA' AND destination == 'SFO' AND date like '01010%' AND delay > 0")
spark.sql("SELECT * FROM virginiablancoravenaweek10filter WHERE origin == 'SEA' AND destination == 'SFO' AND date like '01010%' AND delay > 0").show()


