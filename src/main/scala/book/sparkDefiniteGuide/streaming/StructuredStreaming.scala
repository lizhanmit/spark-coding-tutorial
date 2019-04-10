package book.sparkDefiniteGuide.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object StructuredStreaming {

  case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: BigInt)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("StructuredStreaming")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark.conf.set("spark.sql.shuffle.partitions", 5)

    import spark.implicits._

    val file = "src/main/resources/sparkDefiniteGuide/inputData/activity-data"
    // static version
    println("=== static version ===")
    val staticDF = spark.read.json(file)
    val dataSchema = staticDF.schema
    staticDF.printSchema()

    // streaming version
    // "maxFilesPerTrigger" controls how quickly Spark will read all of the files in the folder
    // here we set it as 1, which is for the purpose of demonstrating how Spark Streaming works, probably not for production
    // NOTE: avoid using schema this way in production where your data may (accidentally) change,
    // instead, use schema inference through setting the configuration "spark.sql.streaming.schemaInference" to true
    println("=== streaming version ===")
    val streamingDF = spark.readStream
      .schema(dataSchema)
      .option("maxFilesPerTrigger", 1)
      .json(file)


    /*
     * count
     */
    val activityCounts = streamingDF.groupBy("gt").count()
    // here you can see the difference between streaming and static style
    val activityQuery = activityCounts.writeStream
      .queryName("activity_counts")  // the name of the in-memory table
      .format("memory")
      .outputMode("complete")
      .start()

    for (i <- 1 to 5) {
      spark.sql("select * from activity_counts").show(3)
      Thread.sleep(1000)
    }

    // must include this statement in production app
    activityQuery.awaitTermination()


    /*
     * selections and filtering
     */
    println("=== selections and filtering ===")
    val simpleTransform = streamingDF.withColumn("stairs", expr("gt like '%stairs%'"))
        .where("stairs")
        .where("gt is not null")
        .select("gt", "model", "arrival_time", "creation_time", "stairs")
        .writeStream
        .queryName("simple_transform")
        .format("memory")
        .outputMode("append")
        .start()

    for (i <- 1 to 5) {
      spark.sql("select * from simple_transform").show(3)
      Thread.sleep(2000)
    }

    simpleTransform.awaitTermination()



    /*
     * aggregations
     */
    println("=== aggregations ===")
    val deviceModelStats = streamingDF.drop("Arrival_Time", "Creation_Time", "Index")
      .cube("gt", "model")
      .avg()
      .writeStream
      .queryName("device_counts")
      .format("memory")
      .outputMode("complete")
      .start()

    for (i <- 1 to 5) {
      spark.sql("select * from device_counts").show(3)
      Thread.sleep(2000)
    }

    deviceModelStats.awaitTermination()


    /*
     * joins
     */
    println("=== joins ===")
    val historicalAgg = staticDF.groupBy("gt", "model").avg()
    val deviceModelStats2 = streamingDF.drop("Arrival_Time", "Creation_Time", "Index")
      .cube("gt", "model")
      .avg()
      .join(historicalAgg, Seq("gt", "model"))
      .writeStream
      .queryName("device_counts_2")
      .format("memory")
      .outputMode("complete")
      .start()

    for (i <- 1 to 5) {
      spark.sql("select * from device_counts_2").show()
      Thread.sleep(2000)
    }

    deviceModelStats2.awaitTermination()


    /*
     * kafka source
     */
    println("=== kafka source ===")
    // subscribe to one topic
    val ds1 = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1, host2:port2")
      .option("subscribe", "topic1")
      .load()

    // subscribe to multiple topics
    val ds2 = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1, host2:port2")
      .option("subscribe", "topic1, topic2")
      .load()

    // subscribe to a pattern of topics
    val ds3 = spark.readStream
      .option("kafka.bootstrap.servers", "host1:port1, host2:port2")
      .option("subscribePattern", "topic.*")
      .load()

    /*
     * kafka sink
     */
    println("=== kafka sink ===")
    ds1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("checkpointLocation", "<locationDir>")
      .option("kafka.bootstrap.servers", "host1:port1, host2:port2")
      .option("topic", "topic1")
      .start()


    /*
     * socket source
     */
    println("=== socket source ===")
    // socket source (only for testing, not in production)
    val socketDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()


    /*
     * console sink (only for testing, not in production)
     */
    println("=== console sink ===")
    val socketConsoleSink = socketDF.writeStream.format("console").start()
    socketConsoleSink.awaitTermination()


    /*
     * trigger
     */
    println("=== trigger ===")
    // processing time trigger
    val activityQuery2 = activityCounts.writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .format("console")
      .outputMode("complete")
      .start()
    activityQuery2.awaitTermination()


    // once trigger
    val activityQuery3 = activityCounts.writeStream
      .trigger(Trigger.Once())
      .format("console")
      .outputMode("complete")
      .start()
    activityQuery3.awaitTermination()


    /*
     * Streaming Dataset API
     */
    println("=== Streaming Dataset API ===")
    val flightDataSchema = spark.read
      .parquet("src/main/resources/sparkDefiniteGuide/inputData/flight-data/2010-summary.parquet")
      .schema
    val flightsDS = spark.readStream
      .schema(flightDataSchema)
      .parquet("src/main/resources/sparkDefiniteGuide/inputData/flight-data/2010-summary.parquet")
      .as[Flight]

    def originIsDestination(flight: Flight): Boolean = {
      flight.DEST_COUNTRY_NAME == flight.ORIGIN_COUNTRY_NAME
    }

    flightsDS.filter(originIsDestination(_))
      .groupByKey(_.DEST_COUNTRY_NAME)
      .count()
      .writeStream
      .queryName("flight_counts")
      .format("memory")
      .outputMode("complete")
      .start()

  }
}
