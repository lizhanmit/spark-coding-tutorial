package book.sparkDefiniteGuide.intro

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.clustering.KMeans

object MLIntro {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("MLIntro")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    spark.conf.set("spark.sql.shuffle.partitions", "5")

    val file = "src/main/resources/sparkDefiniteGuide/inputData/retail-data/by-day/*.csv"

    val staticDataFrame = spark.read
      .format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(file)

    staticDataFrame.printSchema()


    /*
     * prepare trainDataFrame and testDataFrame
     */
    val preppedDataFrame = staticDataFrame
      .na.fill(0)
      .withColumn("day_of_week", date_format($"InvoiceDate", "EEEE"))
      .coalesce(5)

    val trainDataFrame = preppedDataFrame.where("InvoiceDate < '2011-07-01'")
    val testDataFrame = preppedDataFrame.where("InvoiceDate >= '2011-07-01'")

    println(trainDataFrame.count())
    println(testDataFrame.count())


    /*
     * prepare for features and transformation pipeline
     */
    val indexer = new StringIndexer()
      .setInputCol("day_of_week")
      .setOutputCol("day_of_week_index")

    val encoder = new OneHotEncoder()
      .setInputCol("day_of_week_index")
      .setOutputCol("day_of_week_encoded")

    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("UnitPrice", "Quantity", "day_of_week_encoded"))
      .setOutputCol("features")

    val transformationPipeline = new Pipeline()
      .setStages(Array(indexer, encoder, vectorAssembler))


    /*
     * prepare for training
     */
    val fittedPipeline = transformationPipeline.fit(trainDataFrame)
    val transformedTraining = fittedPipeline.transform(trainDataFrame)

    transformedTraining.cache()


    /*
     * prepare for ML model
     */
    // untrained model
    val kmeans = new KMeans()
      .setK(20)
      .setSeed(1L)

    // trained model
    val kmeansModel = kmeans.fit(transformedTraining)
    val trainingCost = kmeansModel.computeCost(transformedTraining)
    println(trainingCost)


    /*
     * test
     */
    val transformedTest = fittedPipeline.transform(testDataFrame)
    val testCost = kmeansModel.computeCost(transformedTest)
    println(testCost)


    // then you could continue to improve the model
    // through layering more preprocessing and hyperparameter tuning
  }
}
