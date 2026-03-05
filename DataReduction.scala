import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DataReduction {

  def main(args: Array[String]): Unit = {

    val basePath = "C:/Users/ASUS/Documents/Level8/BigData/Project/CleanData"
    val inputPath = s"$basePath/2009_clean_FINAL"
    val outputPath = s"$basePath/2009_reduced_FINAL"

    val spark = SparkSession.builder()
      .appName("Airline 2009 - Data Reduction")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "8")
      .getOrCreate()

    // Read integrated dataset
    val df = spark.read.parquet(inputPath)

    val originalRows = df.count()
    val originalCols = df.columns.length
    println(s"Original Rows: $originalRows")
    println(s"Original Columns: $originalCols")

    // 1) Feature selection: drop less useful / redundant columns
    val reducedColsDF = df.drop(
      "OP_CARRIER_FL_NUM",
      "WHEELS_ON",
      "WHEELS_OFF",
      "ARR_TIME",
      "DEP_TIME",
      "TAXI_IN",
      "TAXI_OUT",
      "Unnamed: 27"
    )

    println(s"Columns After Dropping: ${reducedColsDF.columns.length}")

    // 2) Stratified sampling by carrier (keep 30% from each OP_CARRIER)
    val carriers = reducedColsDF
      .select("OP_CARRIER")
      .distinct()
      .as[String]
      .collect()

    val fractions: Map[String, Double] = carriers.map(c => c -> 0.3).toMap

    val sampledDF = reducedColsDF.stat.sampleBy("OP_CARRIER", fractions, seed = 42)

    val sampledRows = sampledDF.count()
    println(s"Rows After Sampling: $sampledRows")

    // 3) Aggregation: summarize by carrier and month
    val aggregatedDF = sampledDF
      .withColumn("MONTH", month(col("FL_DATE")))
      .groupBy("OP_CARRIER", "MONTH")
      .agg(
        count("*").alias("total_flights"),
        avg("ARR_DELAY").alias("avg_arr_delay"),
        avg("DEP_DELAY").alias("avg_dep_delay"),
        avg("DISTANCE").alias("avg_distance")
      )

    val finalRows = aggregatedDF.count()
    val finalCols = aggregatedDF.columns.length
    println(s"Final Rows After Aggregation: $finalRows")
    println(s"Final Columns After Aggregation: $finalCols")

    //  show sample
    aggregatedDF.show(20, truncate = false)

    // Save reduced dataset
    aggregatedDF.write
      .mode("overwrite")
      .parquet(outputPath)

    println(s"Saved reduced dataset to: $outputPath")

    spark.stop()
  }
}