from pyspark import SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import to_date, avg, month, col, collect_list
from pyspark.sql.types import *
import numpy as np

# Removing hard coded password - using os module to import them
import os
import sys

decade = "40"

# Required configuration to load S3/Minio access credentials securely - no hardcoding keys into code
conf = SparkConf()
# conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.3')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider')

conf.set('spark.hadoop.fs.s3a.access.key', os.getenv('SECRETKEY'))
conf.set('spark.hadoop.fs.s3a.secret.key', os.getenv('ACCESSKEY'))
conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio1.service.consul:9000")
conf.set("fs.s3a.path.style.access", "true")
conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set("fs.s3a.connection.ssl.enabled", "false")

# Create SparkSession Object - tell the cluster the FQDN of the host system)
spark = SparkSession.builder.appName("VBR-part-four").config('spark.driver.host','spark-edge-vm0.service.consul').config(conf=conf).getOrCreate()
schema = StructType([
    StructField("WeatherStation", IntegerType(), False),
    StructField("WBAN", IntegerType(), False),
    StructField("ObservationDate", DateType(), False),
    StructField("ObservationHour", IntegerType(), False),
    StructField("Latitude", DoubleType(), False),
    StructField("Longitude", DoubleType(), False),
    StructField("Elevation", IntegerType(), False),
    StructField("WindDirection", IntegerType(), False),
    StructField("WDQualityCode", IntegerType(), False),
    StructField("SkyCeilingHeight", IntegerType(), False),
    StructField("SCQualityCode", IntegerType(), False),
    StructField("VisibilityDistance", IntegerType(), False),
    StructField("VDQualityCode", IntegerType(), False),
    StructField("AirTemperature", DoubleType(), False),
    StructField("ATQualityCode", IntegerType(), False),
    StructField("DewPoint", DoubleType(), False),
    StructField("DPQualityCode", IntegerType(), False),
    StructField("AtmosphericPressure", DoubleType(), False),
    StructField("APQualityCode", IntegerType(), False)]
    )

# parquetdf = spark.read.format("parquet").option("header","true").option("dateFormat", "yyyy-M-d").schema(schema).load("s3a://vblancoravena/"+decade+"-parquet")
parquetdf = spark.read.format("parquet").option("header","true").option("dateFormat", "yyyy-M-d").load("s3a://vblancoravena/"+decade+"-parquet")

## COMPUTE VALUES
parquetdf = parquetdf.withColumn("ObservationMonth", month("ObservationDate"))
print("First 10 records for the original 40 decade dataframe")
parquetdf.show(10)
print("Schema for the original 40 decade dataframe")
parquetdf.printSchema()
# Maximum Temperature Recorded
max_temp = 134
# Minimum Temperature Recorded
min_temp = -129

februaryDF = parquetdf.filter((parquetdf.AirTemperature >= min_temp) & (parquetdf.AirTemperature <= max_temp) & (parquetdf.ObservationMonth == "2"))

nRecords = februaryDF.count()
avgAT = februaryDF.select(avg(col('AirTemperature'))).collect()[0][0]
AT_list = februaryDF.select(collect_list(col('AirTemperature').cast(FloatType()))).first()[0]
medAT = float(np.median(AT_list))
stdvAT = float(np.std(AT_list))

# print("###################################### RESULT ######################################")
# resutlSchema = StructType([
#     StructField("NumberRecords", IntegerType(), False),
#     StructField("AverageAirTemperature", FloatType(), False),
#     StructField("MedianAirTemperature", FloatType(), False),
#     StructField("StandardDeviationAirTemperature", FloatType(), False)]
#     )

# rows = [Row(nRecords, avgAT, medAT, stdvAT)]
# resultDF = spark.createDataFrame(rows,resutlSchema)

# print("###################################### RESULT DATAFRAME ######################################")
# resultDF.show()
# print("###################################### RESULT SCHEMA ######################################")
# resultDF.printSchema()

# print("###################################### SAVING PARQUET ######################################")
# resultDF.write.mode("overwrite").parquet("s3a://vblancoravena/VBR-part-four-answers-parquet")
# print("###################################### PARQUET SAVED ######################################")

print("###################################### RESULTS ######################################")
print("###################################### NUMBER OF RECORDS ######################################")
nRecordsSchema = StructType([StructField("NumberRecords", IntegerType(), False)])
rows = [Row(nRecords)]
nRecordsDF = spark.createDataFrame(rows,nRecordsSchema)
print("###################################### NUMBER OF RECORDS - DATAFRAME ######################################")
nRecordsDF.show()
print("###################################### NUMBER OF RECORDS -  SCHEMA ######################################")
nRecordsDF.printSchema()
print("###################################### NUMBER OF RECORDS - SAVING PARQUET ######################################")
nRecordsDF.write.mode("overwrite").parquet("s3a://vblancoravena/VBR-part-four-answers-count-parquet")
print("###################################### NUMBER OF RECORDS - PARQUET SAVED ######################################")


print("###################################### AVG AIR TEMPERATURE ######################################")
avgATSchema = StructType([StructField("AverageAirTemperature", FloatType(), False)])
rows = [Row(avgAT)]
avgATDF = spark.createDataFrame(rows,avgATSchema)
print("###################################### AVG AIR TEMPERATURE - DATAFRAME ######################################")
avgATDF.show()
print("###################################### AVG AIR TEMPERATURE -  SCHEMA ######################################")
avgATDF.printSchema()
print("###################################### AVG AIR TEMPERATURE - SAVING PARQUET ######################################")
avgATDF.write.mode("overwrite").parquet("s3a://vblancoravena/VBR-part-four-answers-avg-parquet")
print("###################################### AVG AIR TEMPERATURE - PARQUET SAVED ######################################")


print("###################################### MEDIAN AIR TEMPERATURE ######################################")
medATSchema = StructType([StructField("MedianAirTemperature", FloatType(), False)])
rows = [Row(medAT)]
medATDF = spark.createDataFrame(rows,medATSchema)
print("###################################### MEDIAN AIR TEMPERATURE - DATAFRAME ######################################")
medATDF.show()
print("###################################### MEDIAN AIR TEMPERATURE -  SCHEMA ######################################")
medATDF.printSchema()
print("###################################### MEDIAN AIR TEMPERATURE - SAVING PARQUET ######################################")
medATDF.write.mode("overwrite").parquet("s3a://vblancoravena/VBR-part-four-answers-median-parquet")
print("###################################### MEDIAN AIR TEMPERATURE - PARQUET SAVED ######################################")


print("###################################### STANDARD DERIVATION AIR TEMPERATURE ######################################")
stdvATSchema = StructType([StructField("StandardDeviationAirTemperature", FloatType(), False)])
rows = [Row(medAT)]
stdvATDF = spark.createDataFrame(rows,stdvATSchema)
print("###################################### STANDARD DERIVATION AIR TEMPERATURE - DATAFRAME ######################################")
stdvATDF.show()
print("###################################### STANDARD DERIVATION AIR TEMPERATURE -  SCHEMA ######################################")
stdvATDF.printSchema()
print("###################################### STANDARD DERIVATION AIR TEMPERATURE - SAVING PARQUET ######################################")
stdvATDF.write.mode("overwrite").parquet("s3a://vblancoravena/VBR-part-four-answers-stddev-parquet")
print("###################################### STANDARD DERIVATION AIR TEMPERATURE - PARQUET SAVED ######################################")