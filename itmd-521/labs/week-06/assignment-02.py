from pyspark.sql import SparkSession 
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.window import Window

spark = (SparkSession
    .builder
    .appName("assignment-02")
    .getOrCreate())
csv_file = '/home/vagrant/LearningSparkV2/chapter3/data/sf-fire-calls.csv'

schema = StructType([StructField("CallNumber", IntegerType(), False),
StructField("UnitID", StringType(), False),
StructField("IncidentNumber", IntegerType(), False),
StructField("CallType", StringType(), False),
StructField("CallDate", DateType(), False),
StructField("WatchDate", DateType(), False),
StructField("CallFinalDisposition", StringType(), False),
StructField("AvailableDtTm", TimestampType(), False),
StructField("Address", StringType(), False),
StructField("City", StringType(), False),
StructField("Zipcode", IntegerType(), False),
StructField("Battalion", StringType(), False),
StructField("StationArea", IntegerType(), False),
StructField("Box", IntegerType(), False),
StructField("OriginalPriority", IntegerType(), False),
StructField("Priority", IntegerType(), False),
StructField("FinalPriority", IntegerType(), False),
StructField("ALSUnit", BooleanType(), False),
StructField("CallTypeGroup", StringType(), False),
StructField("NumAlarms", IntegerType(), False),
StructField("UnitType", StringType(), False),
StructField("UnitSequenceInCallDispatch", IntegerType(), False),
StructField("FirePreventionDistrict", IntegerType(), False),
StructField("SupervisorDistrict", IntegerType(), False),
StructField("Neighborhood", StringType(), False),
StructField("Location", StringType(), False),
StructField("RowID", StringType(), False),
StructField("Delay", DoubleType(), False)
])

df = spark.read.format("csv").option("header","true").option("timestampFormat", "M/d/yyyy hh:mm:ss a").option("dateFormat", "M/d/yyyy").schema(schema).load(csv_file)

print('****************************** Dataframe schema: ******************************')
print(df.printSchema())

# print('****************************** AvailableDtTm Column: ******************************')
# df.select(f.col("AvailableDtTm")).show()

# 1. What were all the different types of fire calls in 2018? 
print('****************************** 1. What were all the different types of fire calls in 2018? ******************************')
q1 = df.select("CallType").filter(f.year("CallDate") == f.year(f.lit('2018-01-01'))).distinct()
q1.show(truncate=False)

# OUTPUT:
# +-------------------------------+
# |CallType                       |
# +-------------------------------+
# |Elevator / Escalator Rescue    |
# |Alarms                         |
# |Odor (Strange / Unknown)       |
# |Citizen Assist / Service Call  |
# |HazMat                         |
# |Vehicle Fire                   |
# |Other                          |
# |Outside Fire                   |
# |Traffic Collision              |
# |Assist Police                  |
# |Gas Leak (Natural and LP Gases)|
# |Water Rescue                   |
# |Electrical Hazard              |
# |Structure Fire                 |
# |Medical Incident               |
# |Fuel Spill                     |
# |Smoke Investigation (Outside)  |
# |Train / Rail Incident          |
# |Explosion                      |
# |Suspicious Package             |
# +-------------------------------+

# 2. What months within the year 2018 saw the highest number of fire calls? 
print('****************************** 2. What months within the year 2018 saw the highest number of fire calls? ******************************')
df = df.withColumn("CallDateDay", f.dayofmonth("CallDate"))
df = df.withColumn("CallDateMonth", f.month("CallDate"))
df = df.withColumn("CallDateYear", f.year("CallDate"))

df_2018 = df.filter(f.col("CallDateYear")=="2018")

q2 = df_2018.select("CallDateMonth").groupBy("CallDateMonth").count().orderBy("count", ascending=False)
q2.show()

# OUTPUT:
# +-------------+-----+
# |CallDateMonth|count|
# +-------------+-----+
# |           10| 1068|
# |            5| 1047|
# |            3| 1029|
# |            8| 1021|
# |            1| 1007|
# |            7|  974|
# |            6|  974|
# |            9|  951|
# |            4|  947|
# |            2|  919|
# |           11|  199|
# +-------------+-----+

# 3. Which neighborhood in San Francisco generated the most fire calls in 2018? 
print('****************************** 3. Which neighborhood in San Francisco generated the most fire calls in 2018? ******************************')
df_sfo = df_2018.filter((df_2018.City == "SF") | (df_2018.City == "SFO") | (df_2018.City == "SAN FRANCISCO") | (df_2018.City == "San Francisco"))
df_neighborhood = df_sfo.select("Neighborhood").groupBy("Neighborhood").count().orderBy("count", ascending=False)
df_neighborhood_max = df_neighborhood.select(f.max("count"))
max_value_q3 = df_neighborhood_max.first()["max(count)"]

q3 = df_neighborhood.select("Neighborhood").filter(f.col("count") == max_value_q3)
q3.show(truncate=False)

# OUTPUT:
# +------------+
# |Neighborhood|
# +------------+
# |Tenderloin  |
# +------------+

# 4. Which neighborhoods had the worst response times to fire calls in 2018?
print('****************************** 4. Which neighborhoods had the worst response times to fire calls in 2018? ******************************')
df_2018.groupBy("Neighborhood").sum("Delay").orderBy("sum(Delay)", ascending=False)
q4 = df_2018.groupBy("Neighborhood").sum("Delay").orderBy("sum(Delay)", ascending=False)
q4 = q4.withColumnRenamed("sum(Delay)", "sumDelay")
q4.show(truncate=False)

# OUTPUT:
# +------------------------------+------------------+
# |Neighborhood                  |sumDelay          |
# +------------------------------+------------------+
# |Tenderloin                    |5713.416686619998 |
# |South of Market               |4019.9166718899974|
# |Financial District/South Beach|3353.633325319999 |
# |Mission                       |3150.3333302500023|
# |Bayview Hunters Point         |2411.9333414900007|
# |Sunset/Parkside               |1240.1333363999997|
# |Chinatown                     |1182.3499927000007|
# |Western Addition              |1156.0833310099995|
# |Nob Hill                      |1120.9999965      |
# |Hayes Valley                  |980.7833310699995 |
# |Outer Richmond                |955.7999991400001 |
# |Castro/Upper Market           |954.1166644900003 |
# |North Beach                   |898.41666692      |
# |West of Twin Peaks            |880.1000020800002 |
# |Potrero Hill                  |880.0166670599997 |
# |Excelsior                     |834.5166684999999 |
# |Pacific Heights               |798.4666603099995 |
# |Mission Bay                   |686.1666734900002 |
# |Inner Sunset                  |683.46666079      |
# |Marina                        |654.5166688799998 |
# +------------------------------+------------------+


# 5. Which week in the year in 2018 had the most fire calls? 
print('****************************** 5. Which week in the year in 2018 had the most fire calls? ******************************')

df = df.withColumn("CallDateWeekOfYear", f.weekofyear("CallDate"))
df_2018 = df.filter(f.col("CallDateYear")=="2018")

df_week_year = df_2018.select("CallDateWeekOfYear").groupBy("CallDateWeekOfYear").count().orderBy("count", ascending=False)
df_week_year_max = df_week_year.select(f.max("count"))
max_value_q5 = df_week_year_max.first()["max(count)"]
df_week_year.select("CallDateWeekOfYear").filter(f.col("count") == max_value_q5).show(truncate=False)

q5 = df_week_year.select("CallDateWeekOfYear").filter(f.col("count") == max_value_q5)
q5.show(truncate=False)

# OUTPUT:
# +------------------+
# |CallDateWeekOfYear|
# +------------------+
# |22                |
# +------------------+


# 6. Is there a correlation between neighborhood, zip code, and number of fire calls? 
print('****************************** 6. Is there a correlation between neighborhood, zip code, and number of fire calls? ******************************')
df = df.withColumn("NumNeighborhood", f.dense_rank().over(Window.orderBy("Neighborhood")))
df_neighborhood_num = df.select("NumNeighborhood").groupBy("NumNeighborhood").count().orderBy("count", ascending=False)
df_zipcode_q6 = df.select("Zipcode").groupBy("Zipcode").count().orderBy("count", ascending=False)

q6_corr_neighborhood = df_neighborhood_num.stat.corr("NumNeighborhood","count")
q6_corr_zipcode = df_zipcode_q6.stat.corr("Zipcode","count")

# print('CORRELATION NEIGHBORHOOD AND NUM FIRE CALLS: ',q6_corr_neighborhood)
# CORRELATION NEIGHBORHOOD AND NUM FIRE CALLS:  0.09783070828356266 really weak correlation
# print('CORRELATION ZIPCODE AND NUM FIRE CALLS: ',q6_corr_zipcode)
# CORRELATION ZIPCODE AND NUM FIRE CALLS:  0.21485589193343818 weak correlation

# OUTPUT:
# +-------------------+-------------------+
# |corr_neighborhood  |corr_zipcode       |
# +-------------------+-------------------+
# |0.09783070828356266|0.21485589193343818|
# +-------------------+-------------------+

schemaQ6 = StructType([
    StructField("q6_corr_neighborhood", DoubleType(), False),
    StructField("q6_corr_zipcode", DoubleType(), False)
])

dataQ6 = [(q6_corr_neighborhood,q6_corr_zipcode)]
q6 = spark.createDataFrame(data = dataQ6, schema = schemaQ6)
q6.show()

# 7. How can we use Parquet files or SQL tables to store this data and read it back?
print('****************************** 7. How can we use Parquet files or SQL tables to store this data and read it back? ******************************')

parquet_path = "/home/vagrant/vblancoravena/itmd-521/labs/week-06/"
q1.write.parquet(parquet_path+"python_q1.parquet")
q2.write.parquet(parquet_path+"python_q2.parquet")
q3.write.parquet(parquet_path+"python_q3.parquet")
q4.write.parquet(parquet_path+"python_q4.parquet")
q5.write.parquet(parquet_path+"python_q5.parquet")
q6.write.parquet(parquet_path+"python_q6.parquet")


df.show()