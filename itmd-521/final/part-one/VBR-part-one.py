from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import to_date

# Removing hard coded password - using os module to import them
import os
import sys

# Required configuration to load S3/Minio access credentials securely - no hardcoding keys into code
conf = SparkConf()
# conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')
conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.3')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider')

conf.set('spark.hadoop.fs.s3a.access.key', os.getenv('SECRETKEY'))
conf.set('spark.hadoop.fs.s3a.secret.key', os.getenv('ACCESSKEY'))
conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio1.service.consul:9000")
conf.set("fs.s3a.path.style.access", "true")
conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set("fs.s3a.connection.ssl.enabled", "false")

# Create SparkSession Object - tell the cluster the FQDN of the host system)
spark = SparkSession.builder.appName("VBR-minio-read-and-process").config('spark.driver.host','spark-edge-vm0.service.consul').config(conf=conf).getOrCreate()

print("###################################### DECADE 40 ######################################")

# df40 = spark.read.csv('s3a://itmd521/40.txt')

# splitDF40 = df40.withColumn('WeatherStation', df40['_c0'].substr(5, 6)) \
# .withColumn('WBAN', df40['_c0'].substr(11, 5)) \
# .withColumn('ObservationDate',to_date(df40['_c0'].substr(16,8), 'yyyyMMdd')) \
# .withColumn('ObservationHour', df40['_c0'].substr(24, 4).cast(IntegerType())) \
# .withColumn('Latitude', df40['_c0'].substr(29, 6).cast('float') / 1000) \
# .withColumn('Longitude', df40['_c0'].substr(35, 7).cast('float') / 1000) \
# .withColumn('Elevation', df40['_c0'].substr(47, 5).cast(IntegerType())) \
# .withColumn('WindDirection', df40['_c0'].substr(61, 3).cast(IntegerType())) \
# .withColumn('WDQualityCode', df40['_c0'].substr(64, 1).cast(IntegerType())) \
# .withColumn('SkyCeilingHeight', df40['_c0'].substr(71, 5).cast(IntegerType())) \
# .withColumn('SCQualityCode', df40['_c0'].substr(76, 1).cast(IntegerType())) \
# .withColumn('VisibilityDistance', df40['_c0'].substr(79, 6).cast(IntegerType())) \
# .withColumn('VDQualityCode', df40['_c0'].substr(86, 1).cast(IntegerType())) \
# .withColumn('AirTemperature', df40['_c0'].substr(88, 5).cast('float') /10) \
# .withColumn('ATQualityCode', df40['_c0'].substr(93, 1).cast(IntegerType())) \
# .withColumn('DewPoint', df40['_c0'].substr(94, 5).cast('float')) \
# .withColumn('DPQualityCode', df40['_c0'].substr(99, 1).cast(IntegerType())) \
# .withColumn('AtmosphericPressure', df40['_c0'].substr(100, 5).cast('float')/ 10) \
# .withColumn('APQualityCode', df40['_c0'].substr(105, 1).cast(IntegerType())).drop('_c0')

# print("DECADE 40: SCHEMA")
# splitDF40.printSchema()
# print("DECADE 40: SHOW 5")
# splitDF40.show(5)
# print("DECADE 40: SAVING PARQUET")
# splitDF40.write.mode("overwrite").parquet("s3a://vblancoravena/40-parquet")
# print("DECADE 40: PARQUET SAVED")
# print("DECADE 40: SAVING CSV")
# splitDF40.write.mode("overwrite").csv("s3a://vblancoravena/40-csv")
# print("DECADE 40: CSV SAVED")

print("###################################### DECADE 70 ######################################")

df70 = spark.read.option("mode", "DROPMALFORMED").csv('s3a://itmd521/70.txt')

splitDF70 = df70.withColumn('WeatherStation', df70['_c0'].substr(5, 6)) \
.withColumn('WBAN', df70['_c0'].substr(11, 5)) \
.withColumn('ObservationDate',to_date(df70['_c0'].substr(16,8), 'yyyyMMdd')) \
.withColumn('ObservationHour', df70['_c0'].substr(24, 4).cast(IntegerType())) \
.withColumn('Latitude', df70['_c0'].substr(29, 6).cast('float') / 1000) \
.withColumn('Longitude', df70['_c0'].substr(35, 7).cast('float') / 1000) \
.withColumn('Elevation', df70['_c0'].substr(47, 5).cast(IntegerType())) \
.withColumn('WindDirection', df70['_c0'].substr(61, 3).cast(IntegerType())) \
.withColumn('WDQualityCode', df70['_c0'].substr(64, 1).cast(IntegerType())) \
.withColumn('SkyCeilingHeight', df70['_c0'].substr(71, 5).cast(IntegerType())) \
.withColumn('SCQualityCode', df70['_c0'].substr(76, 1).cast(IntegerType())) \
.withColumn('VisibilityDistance', df70['_c0'].substr(79, 6).cast(IntegerType())) \
.withColumn('VDQualityCode', df70['_c0'].substr(86, 1).cast(IntegerType())) \
.withColumn('AirTemperature', df70['_c0'].substr(88, 5).cast('float') /10) \
.withColumn('ATQualityCode', df70['_c0'].substr(93, 1).cast(IntegerType())) \
.withColumn('DewPoint', df70['_c0'].substr(94, 5).cast('float')) \
.withColumn('DPQualityCode', df70['_c0'].substr(99, 1).cast(IntegerType())) \
.withColumn('AtmosphericPressure', df70['_c0'].substr(100, 5).cast('float')/ 10) \
.withColumn('APQualityCode', df70['_c0'].substr(105, 1).cast(IntegerType())).drop('_c0')

print("DECADE 70: SCHEMA")
splitDF70.printSchema()
print("DECADE 70: SHOW 5")
splitDF70.show(5)
print("DECADE 70: SAVING PARQUET")
splitDF70.write.mode("overwrite").parquet("s3a://vblancoravena/70-parquet")
print("DECADE 70: PARQUET SAVED")
print("DECADE 70: SAVING CSV")
splitDF70.write.mode("overwrite").csv("s3a://vblancoravena/70-csv")
print("DECADE 70: CSV SAVED")

print("###################################### DECADE 90 ######################################")

df90 = spark.read.csv('s3a://itmd521/90.txt')

splitDF90 = df90.withColumn('WeatherStation', df90['_c0'].substr(5, 6)) \
.withColumn('WBAN', df90['_c0'].substr(11, 5)) \
.withColumn('ObservationDate',to_date(df90['_c0'].substr(16,8), 'yyyyMMdd')) \
.withColumn('ObservationHour', df90['_c0'].substr(24, 4).cast(IntegerType())) \
.withColumn('Latitude', df90['_c0'].substr(29, 6).cast('float') / 1000) \
.withColumn('Longitude', df90['_c0'].substr(35, 7).cast('float') / 1000) \
.withColumn('Elevation', df90['_c0'].substr(47, 5).cast(IntegerType())) \
.withColumn('WindDirection', df90['_c0'].substr(61, 3).cast(IntegerType())) \
.withColumn('WDQualityCode', df90['_c0'].substr(64, 1).cast(IntegerType())) \
.withColumn('SkyCeilingHeight', df90['_c0'].substr(71, 5).cast(IntegerType())) \
.withColumn('SCQualityCode', df90['_c0'].substr(76, 1).cast(IntegerType())) \
.withColumn('VisibilityDistance', df90['_c0'].substr(79, 6).cast(IntegerType())) \
.withColumn('VDQualityCode', df90['_c0'].substr(86, 1).cast(IntegerType())) \
.withColumn('AirTemperature', df90['_c0'].substr(88, 5).cast('float') /10) \
.withColumn('ATQualityCode', df90['_c0'].substr(93, 1).cast(IntegerType())) \
.withColumn('DewPoint', df90['_c0'].substr(94, 5).cast('float')) \
.withColumn('DPQualityCode', df90['_c0'].substr(99, 1).cast(IntegerType())) \
.withColumn('AtmosphericPressure', df90['_c0'].substr(100, 5).cast('float')/ 10) \
.withColumn('APQualityCode', df90['_c0'].substr(105, 1).cast(IntegerType())).drop('_c0')

print("DECADE 90: SCHEMA")
splitDF90.printSchema()
print("DECADE 90: SHOW 5")
splitDF90.show(5)
print("DECADE 90: SAVING PARQUET")
splitDF90.write.mode("overwrite").parquet("s3a://vblancoravena/90-parquet")
splitDF90.write.mode("overwrite").parquet("s3a://vblancoravena/90-parquet")

print("DECADE 90: PARQUET SAVED")
print("DECADE 90: SAVING CSV")
splitDF90.write.mode("overwrite").csv("s3a://vblancoravena/90-csv")
print("DECADE 90: CSV SAVED")