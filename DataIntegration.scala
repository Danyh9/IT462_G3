import org.apache.spark.sql.{DataFrame, SparkSession}

object DataIntegration {

  def main(args: Array[String]): Unit = {

    // Base path where cleaned quarterly Parquet files are stored
    val base = "C:/Users/ASUS/Documents/Level8/BigData/Project/CleanData"

    val spark = SparkSession.builder()
      .appName("Airline 2009 - Data Integration (Union Quarters)")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "8")
      .getOrCreate()

    // Windows + Hadoop local filesystem workaround (optional but helpful on Windows)
    spark.sparkContext.hadoopConfiguration.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs")
    spark.sparkContext.hadoopConfiguration.set("io.native.lib.available", "false")
    spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true")

    // Read cleaned quarter datasets
    val q1 = spark.read.parquet(s"$base/2009_Q1_clean")
    val q2 = spark.read.parquet(s"$base/2009_Q2_clean")
    val q3 = spark.read.parquet(s"$base/2009_Q3_clean")
    val q4 = spark.read.parquet(s"$base/2009_Q4_clean")

    // Integrate all quarters into one yearly dataset (schema-safe)
    val final2009 = q1.unionByName(q2).unionByName(q3).unionByName(q4)

    // Quick stats (useful for logs / summary)
    val rowCount = final2009.count()
    println(s"Integrated dataset row count (2009 full year): $rowCount")

    // Save integrated dataset
    val outPath = s"$base/2009_clean_FINAL"

   
   final2009.write
    .mode("overwrite")
    .parquet(outPath)

    println(s"Saved integrated dataset to: $outPath")

    spark.stop()
  }
}