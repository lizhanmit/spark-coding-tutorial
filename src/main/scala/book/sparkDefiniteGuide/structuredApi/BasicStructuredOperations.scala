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


    df.cache()
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
      """).show()
    // or
    df.withColumn("withinCountry", expr("DEST_COUNTRY_NAME == ORIGIN_COUNTRY_NAME")).show(3)


    // aggregation
    df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(3)
    // or
    spark.sql(
      """
        select avg(count), count(distinct(DEST_COUNTRY_NAME))
        from dfTable
        limit 3
      """).show()


    /*
     * add a new literal column using lit() function
     * add a column named "One" with value 1 for all rows
     */
    df.select(expr("*"), lit(1).as("One")).show(3)
    // or
    df.selectExpr("*", "1 as One").show(3)
    // or
    spark.sql(
      """
        select *, 1 as One
        from dfTable
        limit 3
      """).show()
    // or
    df.withColumn("One", lit(1)).show(3)


    // rename columns
    df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns.foreach(println)


    /*
     * reserved characters and keywords
     */
    df.withColumn("This Long Column-Name", expr("DEST_COUNTRY_NAME")).show(3)
    // when you refer a column in an expression, use escape character `` around the column name with reserved characters
    df.selectExpr("DEST_COUNTRY_NAME as `This Long Column-Name`").show(3)


    /*
     * remove columns using drop() method
     */
    df.drop("DEST_COUNTRY_NAME").show(3)
    df.drop("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(3)


    /*
     * cast
     */
    df.withColumn("count2", col("count").cast("int")).printSchema()
    // or
    spark.sql(
      """
        select *, cast(count as int) as count2
        from dfTable
      """).printSchema()


    /*
     * filter or where
     */
    df.filter(col("count") < 2).show(3)
    // or
    df.where("count < 2").show(3)
    // or
    spark.sql(
      """
        select *
        from dfTable
        where count < 2
        limit 3
      """).show()

    // multiple where
    // must use =!= operator here,
    // so that you do not just compare the unevaluated column expression to a string but instead to the evaluated one
    df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") =!= "Croatia").show(3)
    // or
    spark.sql(
      """
        select *
        from dfTable
        where count < 2 and ORIGIN_COUNTRY_NAME != "Croatia"
        limit 3
      """).show()


    /*
     * deduplicate using distinct() method
     */
    println(df.select("ORIGIN_COUNTRY_NAME").distinct().count())
    // or
    spark.sql(
      """
        select count(distinct(ORIGIN_COUNTRY_NAME))
        from dfTable
      """).show()


    /*
     * sampling using sample() method
     */
    val withReplacement = false  // if the sampled elements will be re-sampled
    val fraction = 0.5  // how many elements will be sampled, 0.5 refers to 50%
    val seed = 5
    println(df.sample(withReplacement, fraction, seed).count)


    /*
     * random split using randomSplit() method
     * used to create training, validation and test datasets for ML algorithms
     */
    val dataFramesArray = df.randomSplit(Array(0.25, 0.75), seed)
    println(dataFramesArray(0).count())


    /*
     * union DataFrames
     */
    val schema = df.schema
    val newRows = Seq(
      Row("New country", "other country", 5L),
      Row("New country 2", "other country 2", 1L)
    )
    val newRDD = spark.sparkContext.parallelize(newRows)
    val newDF = spark.createDataFrame(newRDD, schema)
    println("number of rows after union: " + df.union(newDF).count())
    println("number of rows before union: " + df.count())


    /*
     * sorting using sort() or orderBy() method
     */
    df.sort("count").show(3)
    // or
    df.orderBy("count").show(3)
    // or
    spark.sql(
      """
        select *
        from dfTable
        order by count
        limit 3
      """).show()
    // sort according to mutiple columns
    df.sort("count", "DEST_COUNTRY_NAME").show(3)
    // specify sort direction
    df.sort(desc("count"), asc("DEST_COUNTRY_NAME")).show(3)
    // or
    df.orderBy(desc("count")).show(3)


    /*
     * repartition and coalesce
     */
    println("number of partitions before repartition: " + df.rdd.getNumPartitions)
    println("number of partitions after repartition: " + df.repartition(5).rdd.getNumPartitions)
    println("number of partitions after coalesce: " + df.repartition(5).coalesce(2).rdd.getNumPartitions)
    df.repartition(5, col("DEST_COUNTRY_NAME")).coalesce(2)

  }
}
