package main.scala.week07

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.{functions => F}
// import spark.sqlContext.implicits._

object assignment_03{
    def main(args: Array[String]){
        val spark = SparkSession
            .builder
            .appName("assignment-03")
            .getOrCreate()

        // if (args.length <= 0){
        //     System.exit(1)
        // }

    val csv_file = "/home/vagrant/LearningSparkV2/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"

    val schema = StructType(Array(
        StructField("date", StringType, false),
        StructField("delay", IntegerType, false),
        StructField("distance", IntegerType, false),
        StructField("origin", StringType, false),
        StructField("destination", StringType, false)
    ))
        

    val week7DF = spark.read.format("csv").option("header",true).schema(schema).load(csv_file)
    week7DF.show(false)
    week7DF.createOrReplaceTempView("us_delay_flights_tbl")

    // PART I
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //// Example 1
    println("************************************************************ EXAMPLE 1 ************************************************************")

    //////// Spark SQL Query
    println("************************************************************ SPARK SQL QUERY ************************************************************")
    spark.sql("""SELECT distance, origin, destination 
    FROM us_delay_flights_tbl WHERE distance > 1000 
    ORDER BY distance DESC""").show(10)

    //////// Spark DataFrame API
    println("************************************************************ SPARK DATAFRAME API ************************************************************")
    week7DF.select(F.col("distance"),F.col("origin"), F.col("destination")).filter(week7DF("distance")>1000).orderBy(F.col("distance").desc).show(10,false)

    //// Example 2
    println("************************************************************ EXAMPLE 2 ************************************************************")

    //////// Spark SQL Query
    println("************************************************************ SPARK SQL QUERY ************************************************************")
    spark.sql("""SELECT date, delay, origin, destination 
    FROM us_delay_flights_tbl 
    WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD' 
    ORDER by delay DESC""").show(10)

    //////// Spark DataFrame API
    println("************************************************************ SPARK DATAFRAME API ************************************************************")
    week7DF.select(F.col("date"), F.col("delay"), F.col("origin"), F.col("destination"))
        .filter(F.col("distance") > 120 && F.col("origin") === "SFO" && F.col("destination") === "ORD")
        .orderBy(F.col("delay").desc)
        .show(10, false)

    //// Example 3 
    println("************************************************************ EXAMPLE 3 ************************************************************")

    //////// Spark SQL Query
    println("************************************************************ SPARK SQL QUERY ************************************************************")
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
    
    //////// Spark DataFrame API
    println("************************************************************ SPARK DATAFRAME API ************************************************************")

    week7DF.select(F.col("delay"), F.col("origin"), F.col("destination"))
        .withColumn("Flight_Delays", F.when(F.col("delay") > 360, "Very Long Delays")
                                .when(F.col("delay").between(120, 360), "Long Delays")
                                .when(F.col("delay").between(60, 119), "Short Delays")
                                .when(F.col("delay").between(1, 59), "Tolerable Delays")
                                .when(F.col("delay") === 0, "No Delays")
                                .otherwise("Early"))
        .orderBy(F.asc("origin"), F.desc("delay"))
        .show(10)
    
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    
    // PART II
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    val week7DF1 = week7DF.withColumn("Date_Format", F.from_unixtime(F.col("date").cast("string"),"MM-dd-yyyy HH:mm:ss"))
    val week7DF2 = week7DF1.withColumn("Date_Format", F.to_timestamp(F.col("Date_Format"),"MM-dd-yyyy HH:mm:ss"))
    val week7DF3 = week7DF2.withColumn("Date_Format_Day", F.dayofmonth(F.col("Date_Format")))
    val week7DF4 = week7DF3.withColumn("Date_Format_Month", F.month(F.col("Date_Format")))
    val week7DF5 = week7DF4.withColumn("Date_Format_Year", F.year(F.col("Date_Format")))
    val week7DF6 = week7DF5.withColumn("Date_Format_Hour", F.hour(F.col("Date_Format")))
    val week7DF7 = week7DF6.withColumn("Date_Format_Minute", F.minute(F.col("Date_Format")))
    val week7DF8 = week7DF7.withColumn("Date_Format_Second", F.second(F.col("Date_Format")))

    week7DF8.show(false)

    week7DF8.createOrReplaceTempView("us_delay_flights_tbl")
    week7DF8
        .write
        .mode(SaveMode.Overwrite)
        .option("path", "./scala")
        .saveAsTable("us_delay_flights_tbl")
    
    //// NO DATA WITH MONTH EQUAL TO 3 !!!!!!!!!!!!!!!!!!!!!!
    week7DF8.select(F.col("date"),F.col("delay"), F.col("origin"), F.col("distance"), F.col("destination"))
        .filter((F.col("origin") === "ORD") && (F.col("Date_Format_Month") === 3) && ((F.col("Date_Format_Day") >= 1) && (F.col("Date_Format_Day") <= 15)))
        .createOrReplaceTempView("chicago_flights")
    
    spark.sql("SELECT * FROM chicago_flights").show(5)
    
    //// SAME EXAMPLE WITH MONTH EQUAL TO 1 !!!!!!!!!!!!!!!!!!!!!!
    println("************************************************************ EXAMPLE IN JANUARY ************************************************************")
    week7DF8.select(F.col("date"),F.col("delay"), F.col("origin"), F.col("distance"), F.col("destination"))
        .filter((F.col("origin") === "ORD") && (F.col("Date_Format_Month") === 1) && ((F.col("Date_Format_Day") >= 1) && (F.col("Date_Format_Day") <= 15)))
        .createOrReplaceTempView("chicago_flights")

    spark.sql("SELECT * FROM chicago_flights").show(5)

    //// COLUMNS FROM US DELAY FLIGHTS TABLE
    println("************************************************************ DATABASE ************************************************************")
    spark.catalog.listDatabases.show(false)
    println("************************************************************ TABLES ************************************************************")
    spark.catalog.listTables.show(false)

    println("************************************************************ COLUMNS FROM US DELAY FLIGHTS TABLE ************************************************************")
    val cols = spark.catalog.listColumns("us_delay_flights_tbl")
    cols.show(false)
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    
    // PART III
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    val week7_part3_1_DF = spark.read.format("csv").option("header",true).schema(schema).load(csv_file)
    val week7_part3_2_DF = week7_part3_1_DF.withColumn("date", F.from_unixtime(F.col("date").cast("string"),"MM-dd-yyyy HH:mm:ss"))
    val week7_part3_DF = week7_part3_2_DF.withColumn("date", F.to_timestamp(F.col("date"),"MM-dd-yyyy HH:mm:ss"))
    week7_part3_DF.show(false)
    
    week7_part3_DF.dtypes.foreach(f=>println(f._1+","+f._2))

    println("************************************************************ JSON ************************************************************")
    val path_json = "./json/scala/departuredelays"
    week7_part3_DF.write.format("json").mode("overwrite").option("path", path_json).saveAsTable("departuredelays")

    println("************************************************************ JSON WITH SNAPPY COMPRESSION ************************************************************")
    val path_json_snappy_compression = "./json_snappy_compression/scala/departuredelays"
    
    // week7_part3_DF.write.format("json").mode("overwrite").option("compression", "snappy").option("path", path_json_snappy_compression).saveAsTable("departuredelays")

    
    println("************************************************************ PARQUET ************************************************************")
    val path_parquet = "./parquet/scala/departuredelays"
    week7_part3_DF.write.format("parquet").mode("overwrite").option("path", path_parquet).saveAsTable("departuredelays")
    
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    
    // PART VI
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    
    val parquet_DF = spark.read.format("parquet").load(path_parquet+"/*")
    val path_parquet_ord = "./parquet/orddeparturedelays/scala"

    parquet_DF.filter(F.col("origin") === "ORD").show(10,false)
    // parquet_DF.write.format("parquet").mode("overwrite").save(path_parquet_ord)
    parquet_DF.write.format("parquet").mode("overwrite").option("path", path_parquet_ord).saveAsTable("orddeparturedelays")

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    

    }   
}