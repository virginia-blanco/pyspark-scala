package main.scala.week5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object week_05{
    def main(args: Array[String]){
        val spark = SparkSession
            .builder
            .appName("Example-3_7")
            .getOrCreate()

        if (args.length <= 0){
            println("Usage Example3_7 <file path to blogs .json>")
            System.exit(1)
        }

        val csv_file = "/home/vagrant/vblancoravena/itmd-521/labs/week-05/Divvy_Trips_2015-Q1.csv"

        // INFERRED
        println("************************************************************ INFERRED ************************************************************")
        val inferredDF = spark.read.option("header",true).csv(csv_file)
        inferredDF.show(false)

        println("****************************** Number of records for inferred scala dataframe:"+ inferredDF.count().toString + "******************************")
        println("****************************** Inferred scala dataframe schema: ******************************")
        println(inferredDF.printSchema)
        println(inferredDF.schema)

        // PROGRAMMATICALLY
        println("************************************************************ PROGRAMMATICALLY ************************************************************")

        val programmaticallySchema = StructType(Array(
            StructField("trip_id", IntegerType, false),
            StructField("starttime", TimestampType, false), 
            StructField("stoptime", TimestampType, false),
            StructField("bikeid", IntegerType, false),
            StructField("tripduration", IntegerType, false),
            StructField("from_station_id", IntegerType, false),
            StructField("from_station_name", StringType, false),
            StructField("to_station_id", IntegerType, false),
            StructField("to_station_name", StringType, false),
            StructField("usertype", StringType, false),
            StructField("gender", StringType, false),
            StructField("birthyear", IntegerType, false)
        ))

        val programmaticallyDF = spark.read.format("csv").option("header",true).option("timestampFormat", "M/d/yyyy H:mm").schema(programmaticallySchema).load(csv_file)
       
        programmaticallyDF.show(false)

        println("****************************** Number of records for programmatically scala dataframe: "+ programmaticallyDF.count().toString + " ******************************")
        println("****************************** Programmatically scala dataframe schema: ******************************")
        println(programmaticallyDF.printSchema)
        println(programmaticallyDF.schema)

        // DDL
        println("************************************************************ DDL ************************************************************")
        val ddl_schema = "trip_id INTEGER, starttime TIMESTAMP, stoptime TIMESTAMP, bikeid INTEGER, tripduration INTEGER, from_station_id INTEGER, from_station_name STRING, to_station_id INTEGER, to_station_name STRING, usertype STRING, gender STRING, birthyear INTEGER"

        val ddlDF = spark.read.format("csv").option("header",true).option("timestampFormat", "M/d/yyyy H:mm").schema(ddl_schema).load(csv_file)
        ddlDF.show(false)

        println("****************************** Number of records for ddl scala dataframe: "+ ddlDF.count().toString + " ******************************")
        println("****************************** DDL scala dataframe schema: ******************************")
        println(ddlDF.printSchema)
        println(ddlDF.schema)
        
    }
}
