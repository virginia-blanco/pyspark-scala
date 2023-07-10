package main.scala.week06

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.expressions.Window


object assignment_02{
    def main(args: Array[String]){
        val spark = SparkSession
            .builder
            .appName("assignment-02")
            .getOrCreate()

        if (args.length <= 0){
            System.exit(1)
        }

        val csv_file = "/home/vagrant/LearningSparkV2/chapter3/data/sf-fire-calls.csv"
        
        val schema = StructType(Array(
            StructField("CallNumber", IntegerType, false),
            StructField("UnitID", StringType, false),
            StructField("IncidentNumber", IntegerType, false),
            StructField("CallType", StringType, false),
            StructField("CallDate", DateType, false),
            StructField("WatchDate", DateType, false),
            StructField("CallFinalDisposition", StringType, false),
            StructField("AvailableDtTm", TimestampType, false),
            StructField("Address", StringType, false),
            StructField("City", StringType, false),
            StructField("Zipcode", DoubleType, false),
            StructField("Battalion", StringType, false),
            StructField("StationArea", IntegerType, false),
            StructField("Box", IntegerType, false),
            StructField("OriginalPriority", IntegerType, false),
            StructField("Priority", IntegerType, false),
            StructField("FinalPriority", IntegerType, false),
            StructField("ALSUnit", BooleanType, false),
            StructField("CallTypeGroup", StringType, false),
            StructField("NumAlarms", IntegerType, false),
            StructField("UnitType", StringType, false),
            StructField("UnitSequenceInCallDispatch", IntegerType, false),
            StructField("FirePreventionDistrict", IntegerType, false),
            StructField("SupervisorDistrict", IntegerType, false),
            StructField("Neighborhood", StringType, false),
            StructField("Location", StringType, false),
            StructField("RowID", StringType, false),
            StructField("Delay", DoubleType, false)
        ))

        val week6DF = spark.read.format("csv").option("header",true).option("timestampFormat", "M/d/yyyy hh:mm:ss a").option("dateFormat", "M/d/yyyy").schema(schema).load(csv_file)
       
        week6DF.show(false)
        println(week6DF.printSchema)
        println(week6DF.schema)


        // 1. What were all the different types of fire calls in 2018? 
        println("****************************** 1. What were all the different types of fire calls in 2018? ******************************")

        val week6DF_new = week6DF
        .withColumn("CallDateDay", F.date_format(F.col("CallDate"), "d"))
        .withColumn("CallDateMonth", F.date_format(F.col("CallDate"), "M"))
        .withColumn("CallDateYear", F.date_format(F.col("CallDate"), "yyyy"))
        

        val DF_2018 = week6DF_new.filter(week6DF_new("CallDateYear") === 2018)
        val q1 = DF_2018.select("CallType").distinct
        q1.show(false)

        //  OUTPUT: 
        // +-------------------------------+
        // |CallType                       |
        // +-------------------------------+
        // |Elevator / Escalator Rescue    |
        // |Alarms                         |
        // |Odor (Strange / Unknown)       |
        // |Citizen Assist / Service Call  |
        // |HazMat                         |
        // |Vehicle Fire                   |
        // |Other                          |
        // |Outside Fire                   |
        // |Traffic Collision              |
        // |Assist Police                  |
        // |Gas Leak (Natural and LP Gases)|
        // |Water Rescue                   |
        // |Electrical Hazard              |
        // |Structure Fire                 |
        // |Medical Incident               |
        // |Fuel Spill                     |
        // |Smoke Investigation (Outside)  |
        // |Train / Rail Incident          |
        // |Explosion                      |
        // |Suspicious Package             |
        // +-------------------------------+

        // 2. What months within the year 2018 saw the highest number of fire calls? 
        println("****************************** 2. What months within the year 2018 saw the highest number of fire calls? ******************************")
        
        val q2 = DF_2018.select("CallDateMonth").groupBy("CallDateMonth").count().orderBy(F.col("count").desc)
        q2.show(false)

        //  OUTPUT: 
        // +-------------+-----+
        // |CallDateMonth|count|
        // +-------------+-----+
        // |10           |1068 |
        // |5            |1047 |
        // |3            |1029 |
        // |8            |1021 |
        // |1            |1007 |
        // |6            |974  |
        // |7            |974  |
        // |9            |951  |
        // |4            |947  |
        // |2            |919  |
        // |11           |199  |
        // +-------------+-----+


        // 3. Which neighborhood in San Francisco generated the most fire calls in 2018? 
        println("****************************** 3. Which neighborhood in San Francisco generated the most fire calls in 2018? ******************************")
        
        val sdo_DF = DF_2018.filter(F.col("City").isin("SF", "SFO", "SAN FRANCISCO", "San Francisco"))
        sdo_DF.select("City", "Neighborhood").show(false)
        
        val neighborhood_DF = sdo_DF.select("Neighborhood").groupBy("Neighborhood").count().orderBy((F.col("count").desc))
        val neighborhood_max = neighborhood_DF.agg(F.max("count"))
        val max_value_Q3 = neighborhood_max.first().getLong(0)

        val q3 = neighborhood_DF.filter(F.col("count") === max_value_Q3).select("Neighborhood")
        q3.show(false)

        //  OUTPUT: 
        // +------------+
        // |Neighborhood|
        // +------------+
        // |Tenderloin  |
        // +------------+

        // 4. Which neighborhoods had the worst response times to fire calls in 2018?
        println("****************************** 4. Which neighborhoods had the worst response times to fire calls in 2018? ******************************")
        
        val step1 = DF_2018.select("Neighborhood", "Delay")
        val step2 = step1.groupBy("Neighborhood").sum("Delay")
        val step3 = step2.orderBy(F.col("sum(Delay)").desc)
        val q4 = step3.withColumnRenamed("sum(Delay)", "sumDelay")
        q4.show(false)

        // OUTPUT:
        // +------------------------------+------------------+
        // |Neighborhood                  |sumDelay          |
        // +------------------------------+------------------+
        // |Tenderloin                    |5713.416686619998 |
        // |South of Market               |4019.9166718899974|
        // |Financial District/South Beach|3353.633325319999 |
        // |Mission                       |3150.3333302500023|
        // |Bayview Hunters Point         |2411.9333414900007|
        // |Sunset/Parkside               |1240.1333363999997|
        // |Chinatown                     |1182.3499927000007|
        // |Western Addition              |1156.0833310099995|
        // |Nob Hill                      |1120.9999965      |
        // |Hayes Valley                  |980.7833310699995 |
        // |Outer Richmond                |955.7999991400001 |
        // |Castro/Upper Market           |954.1166644900003 |
        // |North Beach                   |898.41666692      |
        // |West of Twin Peaks            |880.1000020800002 |
        // |Potrero Hill                  |880.0166670599997 |
        // |Excelsior                     |834.5166684999999 |
        // |Pacific Heights               |798.4666603099995 |
        // |Mission Bay                   |686.1666734900002 |
        // |Inner Sunset                  |683.46666079      |
        // |Marina                        |654.5166688799998 |
        // +------------------------------+------------------+
        
        // 5. Which week in the year in 2018 had the most fire calls? 
        println("****************************** 5. Which week in the year in 2018 had the most fire calls? ******************************")
        val week6DF_new_new = week6DF_new.withColumn("CallDateWeekofYear", F.weekofyear(week6DF_new("CallDate")))   
        // week6DF_new_new.show(false)
        val DF_2018_new = week6DF_new_new.filter(week6DF_new_new("CallDateYear") === 2018)
        // DF_2018_new.show(false)
        val q5 = DF_2018_new.select("CallDateWeekOfYear").groupBy("CallDateWeekOfYear").count().orderBy(F.col("count").desc)
        q5.show(1, false)
       
        // OUTPUT:
        // +------------------+-----+
        // |CallDateWeekOfYear|count|
        // +------------------+-----+
        // |22                |259  |
        // +------------------+-----+

        // 6. Is there a correlation between neighborhood, zip code, and number of fire calls? 
        println("****************************** 6. Is there a correlation between neighborhood, zip code, and number of fire calls? ******************************")
        
        val neighborhoodNum_DF = week6DF.withColumn("NumNeighborhood", F.dense_rank().over(Window.orderBy("Neighborhood")))
        val neighborhoodNum_Q6_DF = neighborhoodNum_DF.select("NumNeighborhood").groupBy("NumNeighborhood").count().orderBy(F.col("count").desc)
        val zipcode_Q6_DF = neighborhoodNum_DF.select("Zipcode").groupBy("Zipcode").count().orderBy(F.col("count").desc)

        val corr_neighborhood_Q6 = neighborhoodNum_Q6_DF.stat.corr("NumNeighborhood", "count")
        val corr_zipcode_Q6 = zipcode_Q6_DF.stat.corr("Zipcode", "count")

        val q6 = spark.createDataFrame(Seq(
          (corr_neighborhood_Q6, corr_zipcode_Q6)
            )).toDF("corr_neighborhood", "corr_zipcode")
        q6.show(false)

        // OUTPUT:
        // +-------------------+-------------------+
        // |corr_neighborhood  |corr_zipcode       |
        // +-------------------+-------------------+
        // |0.09783070828356266|0.21485589193343818|
        // +-------------------+-------------------+

        // 7. How can we use Parquet files or SQL tables to store this data and read it back?
         println("****************************** 7. How can we use Parquet files or SQL tables to store this data and read it back? ******************************")       

        val  parquet_path = "/home/vagrant/vblancoravena/itmd-521/labs/week-06/"
        q1.write.parquet(parquet_path + "scala_q1.parquet")
        q2.write.parquet(parquet_path + "scala_q2.parquet")
        q3.write.parquet(parquet_path + "scala_q3.parquet")
        q4.write.parquet(parquet_path + "scala_q4.parquet")
        q5.write.parquet(parquet_path + "scala_q5.parquet")
        q6.write.parquet(parquet_path + "scala_q6.parquet")
    
    }
}