from pyspark.sql import SparkSession 
from pyspark.sql.types import *
import pyspark.sql.functions as f

spark = (SparkSession
    .builder
    .appName("week-07")
    .getOrCreate())
csv_file = '/home/vagrant/LearningSparkV2/databricks-datasets/learning-spark-v2/flights/departuredelays.csv'

schema = StructType([
    StructField("date", StringType(), False),
    StructField("delay", IntegerType(), False),
    StructField("distance", IntegerType(), False),
    StructField("origin", StringType(), False),
    StructField("destination", StringType(), False)]
    )


df = spark.read.format("csv").option("header","true").schema(schema).load(csv_file)

df.createOrReplaceTempView("us_delay_flights_tbl")

## PART I
#############################################################################################################################################################################################
#### Example 1
print("************************************************************ EXAMPLE 1 ************************************************************")

######## Spark SQL Query
print("************************************************************ SPARK SQL QUERY ************************************************************")
spark.sql("""SELECT distance, origin, destination 
FROM us_delay_flights_tbl WHERE distance > 1000 
ORDER BY distance DESC""").show(10)

######## Spark DataFrame API
print("************************************************************ SPARK DATAFRAME API ************************************************************")
df.select(f.col("distance"),f.col("origin"), f.col("destination")).filter(f.col("distance")>1000).orderBy("distance", ascending=False).show(10,truncate=False)

#### Example 2
print("************************************************************ EXAMPLE 2 ************************************************************")

######## Spark SQL Query
print("************************************************************ SPARK SQL QUERY ************************************************************")
spark.sql("""SELECT date, delay, origin, destination 
FROM us_delay_flights_tbl 
WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD' 
ORDER by delay DESC""").show(10)

######## Spark DataFrame API
print("************************************************************ SPARK DATAFRAME API ************************************************************")
df.select(f.col("date"),f.col("delay"),f.col("origin"), f.col("destination")).filter((f.col("distance") > 120) & (f.col("origin") == "SFO") & (f.col("destination") == "ORD")).orderBy("delay", ascending=False).show(10,truncate=False)

#### Example 3 
print("************************************************************ EXAMPLE 3 ************************************************************")

######## Spark SQL Query
print("************************************************************ SPARK SQL QUERY ************************************************************")
spark.sql("""SELECT delay, origin, destination, 
              CASE
                  WHEN delay > 360 THEN 'Very Long Delays'
                  WHEN delay >= 120 AND delay <= 360 THEN 'Long Delays'
                  WHEN delay >= 60 AND delay < 120 THEN 'Short Delays'
                  WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
                  WHEN delay = 0 THEN 'No Delays'
                  ELSE 'Early'
               END AS Flight_Delays
               FROM us_delay_flights_tbl
               ORDER BY origin, delay DESC""").show(10)

######## Spark DataFrame API
print("************************************************************ SPARK DATAFRAME API ************************************************************")
df.select(f.col("delay"),f.col("origin"),f.col("destination"))\
    .withColumn("Flight_Delays", f.when(f.col("delay") > 360,"Very Long Delays") \
                                 .when((f.col("delay") >= 120) & (f.col("delay") <= 360),"Long Delays") \
                                 .when((f.col("delay") >= 60) & (f.col("delay") < 120),"Short Delays") \
                                 .when((f.col("delay") > 0) & (f.col("delay") < 60),"Tolerable Delays") \
                                 .when(f.col("delay") == 0,"No Delays") \
                                 .otherwise("Early")) \
    .orderBy(f.asc("origin"), f.desc("delay")).show(10,truncate=False)

#############################################################################################################################################################################################
  
## PART II
#############################################################################################################################################################################################
  
df = df.withColumn("Date_Format", f.from_unixtime(f.col("date").cast("string"),"MM-dd-yyyy HH:mm:ss"))
df = df.withColumn("Date_Format", f.to_timestamp("Date_Format","MM-dd-yyyy HH:mm:ss"))
df = df.withColumn("Date_Format_Day", f.dayofmonth("Date_Format"))
df = df.withColumn("Date_Format_Month", f.month("Date_Format"))
df = df.withColumn("Date_Format_Year", f.year("Date_Format"))
df = df.withColumn("Date_Format_Hour", f.hour("Date_Format"))
df = df.withColumn("Date_Format_Minute", f.minute("Date_Format"))
df = df.withColumn("Date_Format_Second", f.second("Date_Format"))

df.show(truncate=False)

df.createOrReplaceTempView("us_delay_flights_tbl")
df.write.mode("overwrite").option("path", "./py").saveAsTable("us_delay_flights_tbl")

#### NO DATA WITH MONTH EQUAL TO 3 !!!!!!!!!!!!!!!!!!!!!!
df.select(f.col("date"),f.col("delay"), f.col("distance"),f.col("origin"), f.col("destination")) \
    .filter((f.col("origin")=="ORD") & (f.col("Date_Format_Month") == 3) & (f.col("Date_Format_Day") >= 1) & (f.col("Date_Format_Day") <= 15)) \
    .createOrReplaceTempView("chicago_flights")

spark.sql("SELECT * FROM chicago_flights").show(5)

#### SAME EXAMPLE WITH MONTH EQUAL TO 1 !!!!!!!!!!!!!!!!!!!!!!
print("************************************************************ EXAMPLE IN JANUARY ************************************************************")
df.select(f.col("date"),f.col("delay"), f.col("distance"),f.col("origin"), f.col("destination")) \
    .filter((f.col("origin")=="ORD") & (f.col("Date_Format_Month") == 1) & (f.col("Date_Format_Day") >= 1) & (f.col("Date_Format_Day") <= 15)) \
    .createOrReplaceTempView("chicago_flights")

spark.sql("SELECT * FROM chicago_flights").show(5)

#### COLUMNS FROM US DELAY FLIGHTS TABLE
print("************************************************************ DATABASE ************************************************************")
print(spark.catalog.listDatabases())

print("************************************************************ TABLES ************************************************************")
print(spark.catalog.listTables())

print("************************************************************ COLUMNS FROM US DELAY FLIGHTS TABLE ************************************************************")
print(spark.catalog.listColumns("us_delay_flights_tbl"))

#############################################################################################################################################################################################

 ## PART III 
#############################################################################################################################################################################################

df_part3 = spark.read.format("csv").option("header","true").schema(schema).load(csv_file)
df_part3 = df_part3.withColumn("date", f.from_unixtime(f.col("date").cast("string"),"MM-dd-yyyy HH:mm:ss"))
df_part3 = df_part3.withColumn("date", f.to_timestamp("date","MM-dd-yyyy HH:mm:ss"))
df_part3.show(truncate=False)

print("************************************************************ DATA TYPES ************************************************************")
for col in df_part3.dtypes:
    print(col[0]+" , "+col[1])

print("************************************************************ JSON ************************************************************")
path_json = "./json/py"
# (df_part3.write.format("json").mode("overwrite").save(path_json))

(df_part3.write.format("json").mode("overwrite").option("path", path_json).saveAsTable("departuredelays"))

print("************************************************************ JSON WITH SNAPPY COMPRESSION ************************************************************")
path_json_snappy_compression = "./json_snappy_compression/py"
# (df_part3.write.format("json").mode("overwrite").option("compression", "snappy").save(path_json_snappy_compression))
try:
    (df_part3.write.format("json").mode("overwrite").option("compression", "snappy").option("path", path_json_snappy_compression).saveAsTable("departuredelays"))
except:
    print("JSON WITH SNAPPY COMPRESSION COULD NOT BE ACCOMPLISHED")

print("************************************************************ PARQUET ************************************************************")
path_parquet = "./parquet/py"
# (df_part3.write.format("parquet").mode("overwrite").save(path_parquet))
(df_part3.write.format("parquet").mode("overwrite").option("path", path_parquet).saveAsTable("departuredelays"))

#############################################################################################################################################################################################

 ## PART VI 
#############################################################################################################################################################################################

df_parquet = (spark.read.format("parquet").load(path_parquet+"/*"))
path_parquet_ord = "./parquet/orddeparturedelays/py"

df_part4 = df_parquet.filter(f.col("origin")=="ORD").show(10,truncate=False)
# (df_part3.write.format("parquet").mode("overwrite").save(path_parquet_ord))
(df_part4.write.format("parquet").mode("overwrite").option("path", path_parquet_ord).saveAsTable("departuredelays"))

#############################################################################################################################################################################################
