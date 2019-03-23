package book.sparkDefiniteGuide.intro

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object StructuredStreamingIntro {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("StructuredStreamingIntro")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    spark.conf.set("spark.sql.shuffle.partitions", "5")

    val file = "src/main/resources/sparkDefiniteGuide/inputData/retail-data/by-day/*.csv"
//    val staticDataFrame = spark.read
//      .option("header", true)
//      .option("inferSchema", true)
//      .csv(file)
    // or
    val staticDataFrame = spark.read.format("csv")
        .option("header", true)
        .option("inferSchema", true)
        .load(file)

    val staticSchema = staticDataFrame.schema
    println(staticSchema)

    //staticDataFrame.createOrReplaceTempView("retail_data")

    staticDataFrame.selectExpr("CustomerId",
        "(UnitPrice * Quantity) as total_cost",
        "InvoiceDate")
      .groupBy(col("CustomerId"), window(col("InvoiceDate"), "1 day"))
      .sum("total_cost")
      .show(5)



    // stimulate streaming
    val streamingDataFrame = spark.readStream
      .schema(staticSchema)  // or you can create a schema through StructType and StructField
      .option("maxFilesPerTrigger", 1)  // specifies the number of files you should read in at once
      .format("csv")
      .option("header", true)
      .load(file)

    println("Is streaming: " + streamingDataFrame.isStreaming)

    val purchaseByCustomerPerHour = streamingDataFrame.selectExpr("CustomerId",
        "(UnitPrice * Quantity) as total_cost",
        "InvoiceDate")
      .groupBy($"CustomerId", window($"InvoiceDate", "1 day"))  // if you use $ to convert string to column, import spark.implicits._
      .sum("total_cost")


    purchaseByCustomerPerHour.writeStream
      .format("memory")  // write to in-memory table
      .queryName("customer_purchases")  // name of the in-memory table for querying purpose
      .outputMode("complete")  // all counts should be in the table
      .start()

    // query the result
    spark.sql(
      """
        select *
        from customer_purchases
        order by sum(total_cost) desc
      """)
      .show(5)
  }
}
