import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler, StandardScaler}

object DataTransformation {

  def main(args: Array[String]): Unit = {

    val inputPath = "C:/Users/ASUS/Documents/Level8/BigData/Project/CleanData/2009_clean_FINAL"
    val outputPath = "C:/Users/ASUS/Documents/Level8/BigData/Project/CleanData/2009_transformed_FINAL"

    val spark = SparkSession.builder()
      .appName("Airline 2009 - Data Transformation")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "8")
      .getOrCreate()

    // Read integrated dataset
    val df0 = spark.read.parquet(inputPath)

    // Transformations / Feature Engineering
    val dfT = df0
      .filter(col("ARR_DELAY").isNotNull)
      .filter(col("CANCELLED") === 0 || col("CANCELLED").isNull)
      .withColumn("label", when(col("ARR_DELAY") >= 15, 1.0).otherwise(0.0))
      .withColumn("DAY_OF_WEEK", dayofweek(col("FL_DATE")).cast("double"))
      .withColumn("IS_WEEKEND", when(col("DAY_OF_WEEK").isin(1.0, 7.0), 1.0).otherwise(0.0))
      .withColumn("MONTH", month(col("FL_DATE")).cast("double"))
      .withColumn("DEP_HOUR_D", col("DEP_HOUR").cast("double"))
      .withColumn("DEP_MINUTE_D", col("DEP_MINUTE").cast("double"))
      .na.fill(0.0, Seq("CARRIER_DELAY","WEATHER_DELAY","NAS_DELAY","SECURITY_DELAY","LATE_AIRCRAFT_DELAY"))

    // Categorical columns
    val catCols = Array("OP_CARRIER","ORIGIN","DEST")

    val indexers = catCols.map(c =>
      new StringIndexer()
        .setInputCol(c)
        .setOutputCol(s"${c}_idx")
        .setHandleInvalid("keep")
    )

    val encoder = new OneHotEncoder()
      .setInputCols(catCols.map(c => s"${c}_idx"))
      .setOutputCols(catCols.map(c => s"${c}_ohe"))
      .setHandleInvalid("keep")

    // Numeric columns to assemble + scale
    val numericCols = Array(
      "CRS_ELAPSED_TIME","DISTANCE","DEP_HOUR_D","DEP_MINUTE_D",
      "CARRIER_DELAY","WEATHER_DELAY","NAS_DELAY","SECURITY_DELAY","LATE_AIRCRAFT_DELAY",
      "DAY_OF_WEEK","IS_WEEKEND","MONTH"
    )

    val numAssembler = new VectorAssembler()
      .setInputCols(numericCols)
      .setOutputCol("numFeatures")

    val scaler = new StandardScaler()
      .setInputCol("numFeatures")
      .setOutputCol("numFeaturesScaled")
      .setWithStd(true)
      .setWithMean(false)

    val finalAssembler = new VectorAssembler()
      .setInputCols(Array("numFeaturesScaled","OP_CARRIER_ohe","ORIGIN_ohe","DEST_ohe"))
      .setOutputCol("features")

    // Build and fit pipeline
    val pipeline = new Pipeline().setStages(indexers ++ Array(encoder, numAssembler, scaler, finalAssembler))
    val model = pipeline.fit(dfT)

    // Final transformed dataset
    val finalDf = model
      .transform(dfT)
      .select("label","features","FL_DATE","OP_CARRIER","ORIGIN","DEST","ARR_DELAY")

    // Sanity check
    val nullFeatures = finalDf.filter(col("features").isNull).count()
    println(s"Null features count: $nullFeatures")

    // Snapshot preview
    finalDf.show(10, truncate = false)

    // Save
    finalDf.write
      .mode("overwrite")
      .parquet(outputPath)

    println(s"Saved transformed dataset to: $outputPath")

    spark.stop()
  }
}