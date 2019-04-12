package book.sparkDefiniteGuide.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object EventTimeAndStatefulProcessing {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("EventTimeAndStatefulProcessing")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark.conf.set("spark.sql.shuffle.partitions", 5)


    val file = "src/main/resources/sparkDefiniteGuide/inputData/activity-data"
    val staticDF = spark.read
      .format("json")
      .load(file)

    staticDF.show(3, false)

    val streamingDF = spark.readStream
      .format("json")
      .schema(staticDF.schema)
      .option("maxFilesPerTrigger", 2)
      .load(file)


    // create Event_Time column based on Creation_Time column
    val dfWithEventTime = streamingDF.selectExpr("*", "cast(cast(Creation_Time as double) / 1000000000 as timestamp) as Event_Time")


    /*
     * event-time processing
     */
    println("=== tumbling windows ===")
    val dfWithEventTimeQuery = dfWithEventTime.groupBy(window(col("Event_Time"), "10 minutes"))
      .count()
      .writeStream
      .queryName("events_per_window")
      .format("memory")
      .outputMode("complete")
      .start()

    for (i <- 1 to 5) {
      spark.sql("select * from events_per_window").show(3, false)
      Thread.sleep(2000)
    }


    println("=== tumbling windows with multi-column aggregation ===")
    val dfWithEventTimeQuery2 = dfWithEventTime.groupBy(window(col("Event_Time"), "10 minutes"), col("User"))
      .count()
      .writeStream
      .queryName("events_per_window_per_user")
      .format("memory")
      .outputMode("complete")
      .start()

    for (i <- 1 to 5) {
      spark.sql("select * from events_per_window_per_user").show(3, false)
      Thread.sleep(2000)
    }


    println("=== sliding time ===")
    // the time window slides every 5 minutes
    val dfWithEventTimeQuery3 = dfWithEventTime.groupBy(window(col("Event_Time"), "10 minutes", "5 minutes"))
      .count()
      .writeStream
      .queryName("events_per_window_sliding")
      .format("memory")
      .outputMode("complete")
      .start()

    for (i <- 1 to 5) {
      spark.sql("select * from events_per_window_sliding").show(3, false)
      Thread.sleep(2000)
    }


    /*
     * watermark
     * specifying watermark is somewhat a must
     * so the above code is just for the purpose of demo, shold be added watermark in production
     */
    println("=== watermark ===")
    // Event_Time delays within 30 mins can be processed
    val dfWithEventTimeQuery4 = dfWithEventTime.withWatermark("Event_Time", "30 minutes")
      .groupBy(window(col("Event_Time"), "10 minutes", "5 minutes"))
      .count()
      .writeStream
      .queryName("events_per_window_sliding_watermark")
      .format("memory")
      .outputMode("complete")
      .start()


    /*
     * deduplication
     */
    println("=== deduplication ===")
    val dfWithEventTimeQuery5 = dfWithEventTime.withWatermark("Event_Time", "5 seconds")
      .dropDuplicates("User", "Event_Time")  // rows with the same "User" and "Event_Time" will be deduplicated
      .groupBy("User")
      .count()
      .writeStream
      .queryName("deduplicated")
      .format("memory")
      .outputMode("completed")
      .start()


    dfWithEventTimeQuery.awaitTermination()
    dfWithEventTimeQuery2.awaitTermination()
    dfWithEventTimeQuery3.awaitTermination()
    dfWithEventTimeQuery4.awaitTermination()
    dfWithEventTimeQuery5.awaitTermination()
  }
}
