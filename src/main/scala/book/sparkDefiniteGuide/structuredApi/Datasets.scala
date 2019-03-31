package book.sparkDefiniteGuide.structuredApi

import org.apache.spark.sql.SparkSession

object Datasets {

  case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: BigInt)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Datasets")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val file = "src/main/resources/sparkDefiniteGuide/inputData/flight-data/2010-summary.parquet/"
    val flightsDF = spark.read.format("parquet").load(file)
    val flightsDS = flightsDF.as[Flight]

    flightsDS.show(3)

    println(flightsDS.first().DEST_COUNTRY_NAME)
  }
}
