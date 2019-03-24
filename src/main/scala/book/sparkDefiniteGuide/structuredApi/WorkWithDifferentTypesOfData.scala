package book.sparkDefiniteGuide.structuredApi

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object WorkWithDifferentTypesOfData {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("WorkWithDifferentTypesOfData")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    spark.conf.set("spark.sql.shuffle.partitions", "5")

    val file = "src/main/resources/sparkDefiniteGuide/inputData/retail-data/by-day/2010-12-01.csv"

    val df = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(file)

    df.printSchema()
    df.createOrReplaceTempView("dfTable")
  }
}
