from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.types import *

# Removing hard coded password - using os module to import them
import os
import sys

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
spark = SparkSession.builder.appName("VBR-minio-read").config('spark.driver.host','spark-edge-vm0.service.consul').config(conf=conf).getOrCreate()

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

print("###################################### DECADE 30 ######################################")
decade30 = "30"

###################################### CSV ######################################
print("###################################### CSV ######################################")
csvdf30 = spark.read.format("csv").option("header","true").option("dateFormat", "yyyy-M-d").schema(schema).load("s3a://vblancoravena/"+decade30+"-csv")

print("Decade: "+decade30+" - First 10 records for the partitioned CSV")
csvdf30.show(10)
print("Decade: "+decade30+" - Schema for the partitioned CSV")
csvdf30.printSchema()

# csvdf.write.format("jdbc").option("url","jdbc:mysql://system31.service.consul:3306/ncdc").option("driver","com.mysql.cj.jdbc.Driver").option("dbtable","VBR"+decade30+"csv").option("user",os.getenv('MYSQLUSER')).option("truncate",True).mode("overwrite").option("password", os.getenv('MYSQLPASS')).save()

# readed_csvdf = spark.read.format("jdbc").option("url", "jdbc:mysql://database-240-vm0.service.consul:3306/ncdc").option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "VBR"+decade30+"csv").option("user", os.getenv('MYSQLUSER')).option("password", os.getenv('MYSQLPASS')).load()
# print("Decade: "+decade30+" - First 10 records for the CSV saved in the DataBase")
# readed_csvdf.show(10)
# print("Decade: "+decade30+" - Schema for the CSV saved in the DataBase")
# readed_csvdf.printSchema()

############################################################################

###################################### JSON ######################################
print("###################################### JSON ######################################")
jsondf = spark.read.format("json").option("header","true").option("dateFormat", "yyyy-M-d").schema(schema).load("s3a://vblancoravena/"+decade30+"-json")

print("Decade: "+decade30+" - First 10 records for the partitioned JSON")
jsondf.show(10)
print("Decade: "+decade30+" - Schema for the partitioned JSON")
jsondf.printSchema()

# jsondf.write.format("jdbc").option("url","jdbc:mysql://system31.service.consul:3306/ncdc").option("driver","com.mysql.cj.jdbc.Driver").option("dbtable","VBR"+decade30+"json").option("user",os.getenv('MYSQLUSER')).option("truncate",True).mode("overwrite").option("password", os.getenv('MYSQLPASS')).save()

# readed_jsondf = spark.read.format("jdbc").option("url", "jdbc:mysql://database-240-vm0.service.consul:3306/ncdc").option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "VBR"+decade30+"json").option("user", os.getenv('MYSQLUSER')).option("password", os.getenv('MYSQLPASS')).load()
# print("Decade: "+decade30+" - First 10 records for the JSON saved in the DataBase")
# readed_jsondf.show(10)
# print("Decade: "+decade30+" - Schema for the JSON saved in the DataBase")
# readed_jsondf.printSchema()
############################################################################

###################################### PARQUET ######################################
print("###################################### PARQUET ######################################")
# parquetdf = spark.read.format("parquet").option("header","true").option("dateFormat", "yyyy-M-d").schema(schema).load("s3a://vblancoravena/"+decade30+"-parquet")
parquetdf = spark.read.format("parquet").option("header","true").option("dateFormat", "yyyy-M-d").load("s3a://vblancoravena/"+decade30+"-parquet")

print("Decade: "+decade30+" - First 10 records for the partitioned PARQUET")
parquetdf.show(10)
print("Decade: "+decade30+" - Schema for the partitioned PARQUET")
parquetdf.printSchema()

# parquetdf.write.format("jdbc").option("url","jdbc:mysql://system31.service.consul:3306/ncdc").option("driver","com.mysql.cj.jdbc.Driver").option("dbtable","VBR"+decade30+"parquet").option("user",os.getenv('MYSQLUSER')).option("truncate",True).mode("overwrite").option("password", os.getenv('MYSQLPASS')).save()

# readed_parquetdf = spark.read.format("jdbc").option("url", "jdbc:mysql://database-240-vm0.service.consul:3306/ncdc").option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "VBR"+decade30+"parquet").option("user", os.getenv('MYSQLUSER')).option("password", os.getenv('MYSQLPASS')).load()
# print("Decade: "+decade30+" - First 10 records for the PARQUET saved in the DataBase")
# readed_parquetdf.show(10)
# print("Decade: "+decade30+" - Schema for the PARQUET saved in the DataBase")
# readed_parquetdf.printSchema()
############################################################################
###################################### SAVING TABLE ######################################
print("###################################### SAVING TABLE ######################################")
# spark.read.format("jdbc").option("url", "jdbc:mysql://database-240-vm0.service.consul:3306/ncdc").option("driver","com.mysql.cj.jdbc.Driver").option("user",os.getenv('MYSQLUSER')).option("password", os.getenv('MYSQLPASS')).option("query", "DROP TABLE IF EXISTS VBRthirties").load()

csvdf30.write.format("jdbc").option("url","jdbc:mysql://database-240-vm0.service.consul:3306/ncdc").option("driver","com.mysql.cj.jdbc.Driver").option("dbtable","VBRthirties").option("user",os.getenv('MYSQLUSER')).option("truncate",True).mode("overwrite").option("password", os.getenv('MYSQLPASS')).save()

readed_csvdf30 = spark.read.format("jdbc").option("url", "jdbc:mysql://database-240-vm0.service.consul:3306/ncdc").option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "VBRthirties").option("user", os.getenv('MYSQLUSER')).option("password", os.getenv('MYSQLPASS')).load()
print("Decade: "+decade30+" - First 10 records for the CSV saved in the DataBase")
readed_csvdf30.show(10)
print("Decade: "+decade30+" - Schema for the CSV saved in the DataBase")
readed_csvdf30.printSchema()

print("###################################### DECADE 40 ######################################")
decade40 = "40"
############################################################################
###################################### CSV ######################################
print("###################################### CSV ######################################")
csvdf40 = spark.read.format("csv").option("header","true").option("dateFormat", "yyyy-M-d").schema(schema).load("s3a://vblancoravena/"+decade40+"-csv")

print("Decade: "+decade40+" - First 10 records for the partitioned CSV")
csvdf40.show(10)
print("Decade: "+decade40+" - Schema for the partitioned CSV")
csvdf40.printSchema()

# csvdf.write.format("jdbc").option("url","jdbc:mysql://system31.service.consul:3306/ncdc").option("driver","com.mysql.cj.jdbc.Driver").option("dbtable","VBR"+decade40+"csv").option("user",os.getenv('MYSQLUSER')).option("truncate",True).mode("overwrite").option("password", os.getenv('MYSQLPASS')).save()

# readed_csvdf = spark.read.format("jdbc").option("url", "jdbc:mysql://database-240-vm0.service.consul:3306/ncdc").option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "VBR"+decade40+"csv").option("user", os.getenv('MYSQLUSER')).option("password", os.getenv('MYSQLPASS')).load()
# print("Decade: "+decade40+" - First 10 records for the CSV saved in the DataBase")
# readed_csvdf.show(10)
# print("Decade: "+decade40+" - Schema for the CSV saved in the DataBase")
# readed_csvdf.printSchema()

############################################################################

###################################### PARQUET ######################################
print("###################################### PARQUET ######################################")
# parquetdf = spark.read.format("parquet").option("header","true").option("dateFormat", "yyyy-M-d").schema(schema).load("s3a://vblancoravena/"+decade40+"-parquet")
parquetdf = spark.read.format("parquet").option("header","true").option("dateFormat", "yyyy-M-d").load("s3a://vblancoravena/"+decade40+"-parquet")

print("Decade: "+decade40+" - First 10 records for the partitioned PARQUET")
parquetdf.show(10)
print("Decade: "+decade40+" - Schema for the partitioned PARQUET")
parquetdf.printSchema()

# parquetdf.write.format("jdbc").option("url","jdbc:mysql://system31.service.consul:3306/ncdc").option("driver","com.mysql.cj.jdbc.Driver").option("dbtable","VBR"+decade40+"parquet").option("user",os.getenv('MYSQLUSER')).option("truncate",True).mode("overwrite").option("password", os.getenv('MYSQLPASS')).save()

# readed_parquetdf = spark.read.format("jdbc").option("url", "jdbc:mysql://database-240-vm0.service.consul:3306/ncdc").option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "VBR"+decade40+"parquet").option("user", os.getenv('MYSQLUSER')).option("password", os.getenv('MYSQLPASS')).load()
# print("Decade: "+decade40+" - First 10 records for the PARQUET saved in the DataBase")
# readed_parquetdf.show(10)
# print("Decade: "+decade40+" - Schema for the PARQUET saved in the DataBase")
# readed_parquetdf.printSchema()
############################################################################

###################################### SAVING TABLE ######################################
print("###################################### SAVING TABLE ######################################")
# spark.read.format("jdbc").option("url", "jdbc:mysql://database-240-vm0.service.consul:3306/ncdc").option("driver","com.mysql.cj.jdbc.Driver").option("user",os.getenv('MYSQLUSER')).option("password", os.getenv('MYSQLPASS')).option("query", "DROP TABLE IF EXISTS VBRforties").load()

csvdf40.write.format("jdbc").option("url","jdbc:mysql://database-240-vm0.service.consul:3306/ncdc").option("driver","com.mysql.cj.jdbc.Driver").option("dbtable","VBRforties").option("user",os.getenv('MYSQLUSER')).option("truncate",True).mode("overwrite").option("password", os.getenv('MYSQLPASS')).save()
readed_csvdf40 = spark.read.format("jdbc").option("url", "jdbc:mysql://database-240-vm0.service.consul:3306/ncdc").option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "VBRforties").option("user", os.getenv('MYSQLUSER')).option("password", os.getenv('MYSQLPASS')).load()
print("Decade: "+decade40+" - First 10 records for the CSV saved in the DataBase")
readed_csvdf40.show(10)
print("Decade: "+decade40+" - Schema for the CSV saved in the DataBase")
readed_csvdf40.printSchema()
############################################################################

print("###################################### DECADE 70 ######################################")
decade70 = "70"
###################################### CSV ######################################
print("###################################### CSV ######################################")
csvdf70 = spark.read.format("csv").option("header","true").option("dateFormat", "yyyy-M-d").schema(schema).load("s3a://vblancoravena/"+decade70+"-csv")

print("Decade: "+decade70+" - First 10 records for the partitioned CSV")
csvdf70.show(10)
print("Decade: "+decade70+" - Schema for the partitioned CSV")
csvdf70.printSchema()

# csvdf.write.format("jdbc").option("url","jdbc:mysql://system31.service.consul:3306/ncdc").option("driver","com.mysql.cj.jdbc.Driver").option("dbtable","VBR"+decade70+"csv").option("user",os.getenv('MYSQLUSER')).option("truncate",True).mode("overwrite").option("password", os.getenv('MYSQLPASS')).save()

# readed_csvdf = spark.read.format("jdbc").option("url", "jdbc:mysql://database-240-vm0.service.consul:3306/ncdc").option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "VBR"+decade70+"csv").option("user", os.getenv('MYSQLUSER')).option("password", os.getenv('MYSQLPASS')).load()
# print("Decade: "+decade70+" - First 10 records for the CSV saved in the DataBase")
# readed_csvdf.show(10)
# print("Decade: "+decade70+" - Schema for the CSV saved in the DataBase")
# readed_csvdf.printSchema()

############################################################################

###################################### PARQUET ######################################
print("###################################### PARQUET ######################################")
# parquetdf = spark.read.format("parquet").option("header","true").option("dateFormat", "yyyy-M-d").schema(schema).load("s3a://vblancoravena/"+decade70+"-parquet")
parquetdf = spark.read.format("parquet").option("header","true").option("dateFormat", "yyyy-M-d").load("s3a://vblancoravena/"+decade70+"-parquet")

print("Decade: "+decade70+" - First 10 records for the partitioned PARQUET")
parquetdf.show(10)
print("Decade: "+decade70+" - Schema for the partitioned PARQUET")
parquetdf.printSchema()

# parquetdf.write.format("jdbc").option("url","jdbc:mysql://system31.service.consul:3306/ncdc").option("driver","com.mysql.cj.jdbc.Driver").option("dbtable","VBR"+decade70+"parquet").option("user",os.getenv('MYSQLUSER')).option("truncate",True).mode("overwrite").option("password", os.getenv('MYSQLPASS')).save()

# readed_parquetdf = spark.read.format("jdbc").option("url", "jdbc:mysql://database-240-vm0.service.consul:3306/ncdc").option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "VBR"+decade70+"parquet").option("user", os.getenv('MYSQLUSER')).option("password", os.getenv('MYSQLPASS')).load()
# print("Decade: "+decade70+" - First 10 records for the PARQUET saved in the DataBase")
# readed_parquetdf.show(10)
# print("Decade: "+decade70+" - Schema for the PARQUET saved in the DataBase")
# readed_parquetdf.printSchema()
############################################################################

###################################### SAVING TABLE ######################################
print("###################################### SAVING TABLE ######################################")
# spark.read.format("jdbc").option("url", "jdbc:mysql://database-240-vm0.service.consul:3306/ncdc").option("driver","com.mysql.cj.jdbc.Driver").option("user",os.getenv('MYSQLUSER')).option("password", os.getenv('MYSQLPASS')).option("query", "DROP TABLE IF EXISTS VBRseventies").load()

csvdf70.write.format("jdbc").option("url","jdbc:mysql://database-240-vm0.service.consul:3306/ncdc").option("driver","com.mysql.cj.jdbc.Driver").option("dbtable","VBRseventies").option("user",os.getenv('MYSQLUSER')).option("truncate",True).mode("overwrite").option("password", os.getenv('MYSQLPASS')).save()

readed_csvdf70 = spark.read.format("jdbc").option("url", "jdbc:mysql://database-240-vm0.service.consul:3306/ncdc").option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "VBRseventies").option("user", os.getenv('MYSQLUSER')).option("password", os.getenv('MYSQLPASS')).load()
print("Decade: "+decade70+" - First 10 records for the CSV saved in the DataBase")
readed_csvdf70.show(10)
print("Decade: "+decade70+" - Schema for the CSV saved in the DataBase")
readed_csvdf70.printSchema()
############################################################################

print("###################################### DECADE 90 ######################################")
decade90 = "90"
###################################### CSV ######################################
print("###################################### CSV ######################################")
csvdf90 = spark.read.format("csv").option("header","true").option("dateFormat", "yyyy-M-d").schema(schema).load("s3a://vblancoravena/"+decade90+"-csv")

print("Decade: "+decade90+" - First 10 records for the partitioned CSV")
csvdf90.show(10)
print("Decade: "+decade90+" - Schema for the partitioned CSV")
csvdf90.printSchema()

# csvdf.write.format("jdbc").option("url","jdbc:mysql://system31.service.consul:3306/ncdc").option("driver","com.mysql.cj.jdbc.Driver").option("dbtable","VBR"+decade90+"csv").option("user",os.getenv('MYSQLUSER')).option("truncate",True).mode("overwrite").option("password", os.getenv('MYSQLPASS')).save()

# readed_csvdf = spark.read.format("jdbc").option("url", "jdbc:mysql://database-240-vm0.service.consul:3306/ncdc").option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "VBR"+decade90+"csv").option("user", os.getenv('MYSQLUSER')).option("password", os.getenv('MYSQLPASS')).load()
# print("Decade: "+decade90+" - First 10 records for the CSV saved in the DataBase")
# readed_csvdf.show(10)
# print("Decade: "+decade90+" - Schema for the CSV saved in the DataBase")
# readed_csvdf.printSchema()

############################################################################

###################################### PARQUET ######################################
print("###################################### PARQUET ######################################")
# parquetdf = spark.read.format("parquet").option("header","true").option("dateFormat", "yyyy-M-d").schema(schema).load("s3a://vblancoravena/"+decade90+"-parquet")
parquetdf = spark.read.format("parquet").option("header","true").option("dateFormat", "yyyy-M-d").load("s3a://vblancoravena/"+decade90+"-parquet")

print("Decade: "+decade90+" - First 10 records for the partitioned PARQUET")
parquetdf.show(10)
print("Decade: "+decade90+" - Schema for the partitioned PARQUET")
parquetdf.printSchema()

# parquetdf.write.format("jdbc").option("url","jdbc:mysql://system31.service.consul:3306/ncdc").option("driver","com.mysql.cj.jdbc.Driver").option("dbtable","VBR"+decade90+"parquet").option("user",os.getenv('MYSQLUSER')).option("truncate",True).mode("overwrite").option("password", os.getenv('MYSQLPASS')).save()

# readed_parquetdf = spark.read.format("jdbc").option("url", "jdbc:mysql://database-240-vm0.service.consul:3306/ncdc").option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "VBR"+decade90+"parquet").option("user", os.getenv('MYSQLUSER')).option("password", os.getenv('MYSQLPASS')).load()
# print("Decade: "+decade90+" - First 10 records for the PARQUET saved in the DataBase")
# readed_parquetdf.show(10)
# print("Decade: "+decade90+" - Schema for the PARQUET saved in the DataBase")
# readed_parquetdf.printSchema()
############################################################################

###################################### SAVING TABLE ######################################
print("###################################### SAVING TABLE ######################################")
# spark.read.format("jdbc").option("url", "jdbc:mysql://database-240-vm0.service.consul:3306/ncdc").option("driver","com.mysql.cj.jdbc.Driver").option("user",os.getenv('MYSQLUSER')).option("password", os.getenv('MYSQLPASS')).option("query", "DROP TABLE IF EXISTS VBRnineties").load()

csvdf90.write.format("jdbc").option("url","jdbc:mysql://database-240-vm0.service.consul:3306/ncdc").option("driver","com.mysql.cj.jdbc.Driver").option("dbtable","VBRnineties").option("user",os.getenv('MYSQLUSER')).option("truncate",True).mode("overwrite").option("password", os.getenv('MYSQLPASS')).save()
readed_csvdf90 = spark.read.format("jdbc").option("url", "jdbc:mysql://database-240-vm0.service.consul:3306/ncdc").option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "VBRnineties").option("user", os.getenv('MYSQLUSER')).option("password", os.getenv('MYSQLPASS')).load()
print("Decade: "+decade90+" - First 10 records for the CSV saved in the DataBase")
readed_csvdf90.show(10)
print("Decade: "+decade90+" - Schema for the CSV saved in the DataBase")
readed_csvdf90.printSchema()
############################################################################