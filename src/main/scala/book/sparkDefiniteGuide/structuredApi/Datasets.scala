package book.sparkDefiniteGuide.structuredApi

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Datasets {

  case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: BigInt)
  case class FlightMetadata(count: BigInt, randomData: BigInt)

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

    /*
     * actions
     */
    println("=== actions ===")
    flightsDS.show(3)

    println(flightsDS.first().DEST_COUNTRY_NAME)


    /*
     * transformations
     */
    println("=== transformations ===")
    def originIsDestination(flight_row: Flight): Boolean = {
      flight_row.DEST_COUNTRY_NAME == flight_row.ORIGIN_COUNTRY_NAME
    }

    // filter
    flightsDS.filter(originIsDestination(_)).show(3)

    // map
    flightsDS.map(_.DEST_COUNTRY_NAME).show(3)
    // or
    flightsDF.select(col("DEST_COUNTRY_NAME")).show(3)


    /*
     * joins
     */
    println("=== joins ===")

    val flightsMetaDS = spark.range(500)
      .map((_, scala.util.Random.nextLong()))  // one column becomes two columns
      .withColumnRenamed("_1", "count")
      .withColumnRenamed("_2", "randomData")
      .as[FlightMetadata]

    val flightsDS2 = flightsDS.joinWith(flightsMetaDS, flightsDS.col("count") === flightsMetaDS.col("count"))
    flightsDS2.show(3, false)

    flightsDS2.selectExpr("_1.DEST_COUNTRY_NAME").show(3, false)


    /*
     * grouping and aggregations
     */
    println("=== grouping and aggregations ===")
    flightsDS.groupBy("DEST_COUNTRY_NAME").count().show(false)
    // or
    flightsDS.groupByKey(_.DEST_COUNTRY_NAME).count().show(false)
    // or
    def grpSum2(f: Flight): Int = { 1 }
    flightsDS.groupByKey(_.DEST_COUNTRY_NAME).mapValues(grpSum2).count().show(false)


    def grpSum(countryName: String, values: Iterator[Flight]) = {
      values.dropWhile(_.count < 5).map((countryName, _))
    }
    flightsDS.groupByKey(_.DEST_COUNTRY_NAME).flatMapGroups(grpSum).show(3, false)


    def sum2(left: Flight, right: Flight) = {
      Flight(left.DEST_COUNTRY_NAME, null, left.count + right.count)
    }
    flightsDS.groupByKey(_.DEST_COUNTRY_NAME).reduceGroups(sum2(_, _)).show(false)

  }
}
