package book.sparkDefiniteGuide.intro

import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object DataFrameIntro {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("DataFrameIntro")
      .master("local")
      .getOrCreate()

    val file = "src/main/resources/sparkDefiniteGuide/inputData/flight-data/csv/2015-summary.csv"
    val flightData2015 = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(file)

    // by default, when performing a shuffle, Spark outputs 200 shuffle partitions
    // here we change it to 5
    spark.conf.set("spark.sql.shuffle.partitions", "5")

    // use explain() method to see the plan that Spark builds to execute sort transformation
    flightData2015.sort("count").explain()

    flightData2015.sort("count").take(3).foreach(println)



    // before using sql to manipulate the DataFrame
    // you need to register the DataFrame as a table or view
    flightData2015.createOrReplaceTempView("flight_data_2015")

    val sqlWay = spark.sql(
      """
        select DEST_COUNTRY_NAME, count(*)
        from flight_data_2015
        group by DEST_COUNTRY_NAME
      """)
    // or
    val dataFrameWay = flightData2015.groupBy("DEST_COUNTRY_NAME").count()

    sqlWay.explain()
    dataFrameWay.explain()


    // max function
    spark.sql("select max(count) from flight_data_2015").take(1).foreach(println)
    // or
    flightData2015.select(max("count")).take(1).foreach(println)



    // top 5 destination countries
    val maxSql = spark.sql(
      """
        select DEST_COUNTRY_NAME, sum(count) as destination_total
        from flight_data_2015
        group by DEST_COUNTRY_NAME
        order by sum(count) desc
        limit 5
      """)
    maxSql.show()
    // or
    flightData2015.groupBy("DEST_COUNTRY_NAME")
      .sum("count")
      .withColumnRenamed("sum(count)", "destination_total")
      .sort(desc("sum(count)"))
      .limit(5)
      .show()


  }
}
