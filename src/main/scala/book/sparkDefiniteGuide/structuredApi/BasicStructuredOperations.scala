package book.sparkDefiniteGuide.structuredApi

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, Metadata, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

object BasicStructuredOperations {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("basicStructuredOperations")
      .master("local")
      .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", "5")

    import spark.implicits._

    val file = "src/main/resources/sparkDefiniteGuide/inputData/flight-data/json/2015-summary.json"
    // schema-on-read
    val df = spark.read.format("json").load(file)
    df.printSchema()

    // define schema manually
    val myManualSchema = StructType(Array(
      StructField("DEST_COUNTRY_NAME", StringType, true),
      StructField("ORIGIN_COUNTRY_NAME", StringType, true),
      StructField("count", LongType, false, Metadata.fromJson("{\"hello\":\"world\"}"))
    ))
    val dfWithManualSchema = spark.read.format("json").schema(myManualSchema).load(file)
    dfWithManualSchema.printSchema()
    dfWithManualSchema.columns.foreach(println)



    /*
     * create a DataFrame from a Row and manually defined schema
     */
    val myManualSchema2 = new StructType(Array(
      new StructField("c1", StringType, true),
      new StructField("c2", StringType, true),
      new StructField("c3", LongType, false)
    ))
    val myRows = Seq(Row("hello", null, 1L))
    val myRDD = spark.sparkContext.parallelize(myRows)
    val myDF = spark.createDataFrame(myRDD, myManualSchema2)
    myDF.printSchema()
    myDF.show()


    df.createOrReplaceTempView("dfTable")


    /*
     * select columns
     */
    df.select("DEST_COUNTRY_NAME").show(3)
    df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(3)
    df.select(expr("DEST_COUNTRY_NAME as destination")).show(3)
    df.select(expr("DEST_COUNTRY_NAME").alias("destination")).show(3)
    df.selectExpr("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(3)


    /*
     * add a new column
     */
    df.selectExpr("*", "(DEST_COUNTRY_NAME == ORIGIN_COUNTRY_NAME) as withinCountry").show(3)
    //or
    spark.sql(
      """
        select *, (DEST_COUNTRY_NAME == ORIGIN_COUNTRY_NAME) as withinCountry
        from dfTable
        limit 3
      """).show(3)


    // aggregation
    df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(3)
    // or
    spark.sql(
      """
        select avg(count), count(distinct(DEST_COUNTRY_NAME))
        from dfTable
        limit 3
      """).show(3)
  }
}
