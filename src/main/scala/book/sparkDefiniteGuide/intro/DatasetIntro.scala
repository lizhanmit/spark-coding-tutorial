package book.sparkDefiniteGuide.intro

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DatasetIntro {

  // used for Datasets
  case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: BigInt)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("DatasetIntro")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val file = "src/main/resources/sparkDefiniteGuide/inputData/flight-data/2010-summary.parquet"
    val flightsDF = spark.read.parquet(file)
    val flights = flightsDF.as[Flight]

    flights.filter(_.ORIGIN_COUNTRY_NAME != "Canada").take(5).foreach(println)


  }
}
