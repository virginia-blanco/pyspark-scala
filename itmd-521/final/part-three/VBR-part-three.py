from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.types import *
import time

# Removing hard coded password - using os module to import them
import os
import sys

time0 = time.perf_counter()
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
spark = SparkSession.builder.appName("VBR-part-three").config('spark.driver.host','spark-edge-vm0.service.consul').config(conf=conf).getOrCreate()
time1 = time.perf_counter()
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

parquetdf = spark.read.format("parquet").option("header","true").option("dateFormat", "yyyy-M-d").load("s3a://vblancoravena/"+decade+"-parquet")
print("DECADE 40: SCHEMA")
parquetdf.printSchema()
print("DECADE 40: SHOW 5")
parquetdf.show(5)

time2 = time.perf_counter()
print("###################################### RUNTIME ######################################")
print("SINCE THE BEGING OF THE SCRIPT: ", (time2-time0), "s")
print("SINCE BEFORE THE SCHEMA: ", (time2-time1), "s")
