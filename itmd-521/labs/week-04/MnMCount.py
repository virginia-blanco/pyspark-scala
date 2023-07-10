import sys
from pyspark.sql import SparkSession

if __name__ == "_main_":
    if len(sys.argv) != 2:
        print("Usage mnmcount <file>" , file=sys.stderr)
        sys.exit(-1)

    # Build SparkSession 
    # Just one SparkSession per JVM
    spark = (SparkSession
    .builder
    .appName("PythonMnMcount")
    .getOrCreate())

    # Get data from command-line arguments
    mnm_file = sys.argv[1]

    # create Spark dataframe by reading file
    mnm_df=(spark.read.format("csv")
        .option("header","true")
        .option("inferSchema","true")
        .load(mnm_file))

    # Select "State", "Color", and "Count"
    # groupBy "State", and "Color"
    # Sum count
    # orderBy descending sum count
    count_mnm_df = (mnm_df
        .select("State","Color","Count")
        .groupBy("State","Color")
        .sum("Count")
        .orderBy("sum(Count)", ascending=False))

    # show will trigger the query to be executed
    count_mnm_df.show(n=60, truncate=False)
    print("Total Rows = %d" % (count_mnm_df.count()))
    
    # query for State California filtering by where(mnm_df.State=="CA")
    ca_count_mnm_df = (mnm_df
        .select("State", "Color", "Count")
        .where(mnm_df.State == "CA")
        .groupBy("State", "Color")
        .sum("Count")
        .orderBy("sum(Count)", ascending =False))

    ca_count_mnm_df.show(n=10, truncate=False)
    # stop SparkSession
    spark.stop()