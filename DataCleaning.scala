import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object DataCleaning {

  // Function to standardize columns and remove invalid records
  def fixErrors(df: DataFrame): DataFrame = {

    // Standardize carrier and airport codes (trim + uppercase)
    val standardized = df
      .withColumn("OP_CARRIER", upper(trim(col("OP_CARRIER"))))
      .withColumn("ORIGIN", upper(trim(col("ORIGIN"))))
      .withColumn("DEST", upper(trim(col("DEST"))))

    // Remove logically impossible values
    val filtered = standardized.filter(
      col("DISTANCE") > 0 &&
      col("CRS_ELAPSED_TIME") > 0 &&
      col("CRS_DEP_TIME").between(0, 2359) &&
      (col("CRS_DEP_TIME") % 100) < 60
    )

    // Keep only valid 3-letter airport codes
    filtered
      .filter(col("ORIGIN").rlike("^[A-Z]{3}$"))
      .filter(col("DEST").rlike("^[A-Z]{3}$"))
  }

  def main(args: Array[String]): Unit = {

    // Input and output paths
    val inputPath      = "C:/Users/ASUS/Documents/Level8/BigData/Project/Data/2009.csv"
    val outputBasePath = "C:/Users/ASUS/Documents/Level8/BigData/Project/CleanData"

    // Select quarter to process (1, 2, 3, or 4)
    val quarter = 4

    val spark = SparkSession.builder()
      .appName("Airline 2009 Cleaning - Quarter")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "8") // Reduce shuffle partitions to lower memory usage
      .getOrCreate()

    // Windows + Hadoop local filesystem workaround
    spark.sparkContext.hadoopConfiguration.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs")
    spark.sparkContext.hadoopConfiguration.set("io.native.lib.available", "false")
    spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true")

    // Read full 2009 dataset and convert FL_DATE to Date type
    val rawdf0 = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv(inputPath)
      .withColumn("FL_DATE", to_date(col("FL_DATE"), "yyyy-MM-dd"))

    // Filter dataset by selected quarter
    val rawdf = quarter match {
      case 1 =>
        rawdf0.filter(month(col("FL_DATE")) <= 3) // Jan–Mar
      case 2 =>
        rawdf0.filter(month(col("FL_DATE")) > 3 && month(col("FL_DATE")) <= 6) // Apr–Jun
      case 3 =>
        rawdf0.filter(month(col("FL_DATE")) > 6 && month(col("FL_DATE")) <= 9) // Jul–Sep
      case 4 =>
        rawdf0.filter(month(col("FL_DATE")) > 9) // Oct–Dec
      case _ =>
        rawdf0 // fallback (process full year)
    }

    println(s"=== Cleaning year 2009 - Quarter $quarter ===")

    val beforeRows = rawdf.count()
    println(s"Rows BEFORE cleaning (this quarter): $beforeRows")

    // 1) Remove rows with missing ARR_DELAY
    val dfNullsClean = rawdf.na.drop(Seq("ARR_DELAY"))

    // 2) Remove invalid or inconsistent records
    val dfErrorClean = fixErrors(dfNullsClean)

    // 3) Remove duplicate flights based on key columns
    val keyCols = Seq("FL_DATE","OP_CARRIER","OP_CARRIER_FL_NUM","ORIGIN","DEST","CRS_DEP_TIME")
    val dfDup = dfErrorClean.dropDuplicates(keyCols)

    // 4) Handle outliers by capping ARR_DELAY at 1000 minutes
    val dfOutlier = dfDup.withColumn(
      "ARR_DELAY",
      when(col("ARR_DELAY") > 1000, 1000).otherwise(col("ARR_DELAY"))
    )

    // 5) Feature engineering: extract hour and minute from departure time
    val dfTime = dfOutlier
      .withColumn("DEP_HOUR",   floor(col("CRS_DEP_TIME") / 100))
      .withColumn("DEP_MINUTE", col("CRS_DEP_TIME") % 100)

    val finalRows = dfTime.count()
    println(s"Final Cleaned Rows (Quarter $quarter): $finalRows")
    println(s"Total Rows removed (this quarter): ${beforeRows - finalRows}")

    // Save cleaned quarter as Parquet
    val dfWrite = dfTime.repartition(4)

    val outputPath = s"$outputBasePath/2009_Q${quarter}_clean"

    dfWrite.write
      .mode("overwrite")
      .parquet(outputPath)

    println(s"Saved cleaned data for 2009 Q$quarter to: $outputPath")

    spark.stop()
  }
}