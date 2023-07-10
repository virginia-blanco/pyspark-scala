from pyspark.sql import SparkSession 
from pyspark.sql.types import *

spark = (SparkSession
    .builder
    .appName("AuthorsAges")
    .getOrCreate())
csv_file = '/home/vagrant/vblancoravena/itmd-521/labs/week-05/Divvy_Trips_2015-Q1.csv'

## INFERRED
print('************************************************************ INFERRED ************************************************************')
inferred_df = spark.read.format("csv").option("header","true").load(csv_file)
print('****************************** Number of records for inferred python dataframe:', inferred_df.count(), '******************************')
print('****************************** Inferred python dataframe schema: ******************************')
print(inferred_df.printSchema())

inferred_df.show()

## PROGRAMMATICALLY
print('************************************************************ PROGRAMMATICALLY ************************************************************')
programmatically_schema = StructType([StructField("trip_id", IntegerType(), False),
    StructField("starttime", TimestampType(), False),
    StructField("stoptime", TimestampType(), False),
    StructField("bikeid", IntegerType(), False),
    StructField("tripduration", IntegerType(), False),
    StructField("from_station_id", IntegerType(), False),
    StructField("from_station_name", StringType(), False),
    StructField("to_station_id", IntegerType(), False),
    StructField("to_station_name", StringType(), False),
    StructField("usertype", StringType(), False),
    StructField("gender", StringType(), False),
    StructField("birthyear", IntegerType(), False)])

programmatically_df = spark.read.format("csv").option("header","true").option("timestampFormat", "M/d/yyyy H:mm").schema(programmatically_schema).load(csv_file)

print('****************************** Number of records for programmatically python dataframe:', programmatically_df.count(), '******************************')
print('****************************** Programmatically python dataframe schema: ******************************')
print(programmatically_df.printSchema())

programmatically_df.show()

## DDL
print('************************************************************ DDL ************************************************************')
ddl_schema = "trip_id INTEGER, starttime TIMESTAMP, stoptime TIMESTAMP,  \
    bikeid INTEGER, tripduration INTEGER, from_station_id INTEGER, \
    from_station_name STRING, to_station_id INTEGER, to_station_name STRING, \
    usertype STRING, gender STRING, birthyear INTEGER"

ddl_df = spark.read.format("csv").option("header","true").option("timestampFormat", "M/d/yyyy H:mm").schema(ddl_schema).load(csv_file)

print('****************************** Number of records for ddl python dataframe:', ddl_df.count(), '******************************')
print('****************************** DDL python dataframe schema: ******************************')
print(ddl_df.printSchema())

ddl_df.show()
