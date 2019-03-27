package book.sparkDefiniteGuide.structuredApi

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Aggregations {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("Aggregations")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val file = "src/main/resources/sparkDefiniteGuide/inputData/retail-data/all/*.csv"
    val df = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(file)
      .coalesce(5)  // repartition the data to have far fewer partitions

    df.cache()
    df.createOrReplaceTempView("dfTable")


    /*
     * aggregation
     */
    println("=== aggregation ===")
    // count
    // `count(*)` will count null values (including rows containing all nulls)
    // `count("<columnName>")` will not count null values
    println(df.count())  // action, will return result immediately, here count() is a method
    df.select(count("StockCode")).show()  // transformation, lazy evaluation, here count() is a function
    // or
    spark.sql("select count(StockCode) from dfTable").show()


    // countDistinct
    df.select(countDistinct("StockCode")).show()
    // or
    spark.sql("select count(distinct StockCode) from dfTable").show()


    // approx_count_distinct
    df.select(approx_count_distinct("StockCode", 0.1)).show()
    // or
    spark.sql("select approx_count_distinct(StockCode, 0.1) from dfTable").show()


    // first and last
    df.select(first("StockCode"), last("StockCode")).show()
    // or
    spark.sql("select first(StockCode), last(StockCode) from dfTable").show()


    // min and max
    df.select(min("Quantity"), max("Quantity")).show()
    // or
    spark.sql("select min(Quantity), max(Quantity) from dfTable").show()


    // sum
    df.select(sum("Quantity")).show()
    // or
    spark.sql("select sum(Quantity) from dfTable").show()

    // sumDistinct
    df.select(sumDistinct("Quantity")).show()
    // or
    spark.sql("select sum(distinct Quantity) from dfTable").show()


    // avg
    df.select(
      count("Quantity").alias("total_transactions"),
      sum("Quantity").alias("total_purchases"),
      avg("Quantity").alias("avg_purchases"),
      expr("mean(Quantity)").alias("mean_purchases")
    ).selectExpr("total_purchases / total_transactions", "avg_purchases", "mean_purchases")
      .show()


    // variance and standard deviation
    df.select(
      var_pop("Quantity"),
      var_samp("Quantity"),
      variance("Quantity"),  // default is var_samp()
      stddev_pop("Quantity"),
      stddev_samp("Quantity"),
      stddev("Quantity")).show()  // default is stddev_samp()
    // or
    spark.sql("select var_pop(Quantity), var_samp(Quantity), variance(Quantity), stddev_pop(Quantity), stddev_samp(Quantity), stddev(Quantity) from dfTable").show()


    // skewness and kurtosis
    df.select(skewness("Quantity"), kurtosis("Quantity")).show()
    // or
    spark.sql("select skewness(Quantity), kurtosis(Quantity) from dfTable").show()


    // covariance and correlation
    df.select(
      corr("InvoiceNo", "Quantity"),
      covar_pop("InvoiceNo", "Quantity"),
      covar_samp("InvoiceNo", "Quantity")
    ).show()
    // or
    spark.sql("select corr(InvoiceNo, Quantity), covar_pop(InvoiceNo, Quantity), covar_samp(InvoiceNo, Quantity) from dfTable").show()
    // population and sample are not applicable for corr()


    // collect column values to a list or set
    df.agg(collect_list("Country"), collect_set("Country")).show()
    // or
    df.select(collect_list("Country"), collect_set("Country")).show()
    // or
    spark.sql("select collect_list(Country), collect_set(Country) from dfTable").show()


    /*
     * grouping
     */
    println("=== group ===")
    df.groupBy("InvoiceNo", "CustomerId").count().show()  // count method. "InvoiceNo", "CustomerId" column will be displayed
    // or
    spark.sql("select InvoiceNo, CustomerId, count(*) from dfTable group by InvoiceNo, CustomerId").show()

    df.groupBy("InvoiceNo")
      .agg(count("Quantity").alias("quan"), expr("count(Quantity)"))
      .show()
    // or
    spark.sql(
      """
        |select InvoiceNo, count(Quantity) as quan, count(Quantity)
        |from dfTable
        |group by InvoiceNo
      """.stripMargin).show()

    df.groupBy("InvoiceNo").agg("Quantity" -> "avg", "Quantity" -> "stddev_pop").show(3)
    // or
    df.groupBy("InvoiceNo").agg(avg("Quantity"), stddev_pop("Quantity")).show(3)
    // or
    spark.sql(
      """
        |select InvoiceNo, avg(Quantity), stddev_pop(Quantity)
        |from dfTable
        |group by InvoiceNo
        |limit 3
      """.stripMargin).show()


    // window functions
    val dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))
    dfWithDate.cache()
    dfWithDate.createOrReplaceTempView("dfWithDateTable")

    val windowSpec = Window.partitionBy("CustomerId", "date")
      .orderBy(col("Quantity").desc)
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)  // all previous rows up to the current row

    val maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)
    val purchaseDenseRank = dense_rank().over(windowSpec)
    val purchaseRank = rank().over(windowSpec)
    dfWithDate.where("CustomerId is not null")
      .orderBy("CustomerId")
      .select(
        col("CustomerId"),
        col("date"),
        col("Quantity"),
        maxPurchaseQuantity.alias("maxPurchaseQuantity"),
        purchaseDenseRank.alias("quantityDenseRank"),
        purchaseRank.alias("quantityRank")
      ).show(false)


    // grouping sets
    val dfNoNull = dfWithDate.drop()  // When performing grouping sets, if you do not filter out null values, you will get incorrect results. This applies to cubes, rollups, and grouping sets.
    dfNoNull.cache()
    dfNoNull.createOrReplaceTempView("dfNoNullTable")
    // get the total quantity of all stock codes and customers
    dfNoNull.groupBy(col("CustomerId"), col("StockCode"))
      .agg(sum("Quantity"))
      .sort(desc("CustomerId"), desc("StockCode"))
      .show(false)
    // or
    spark.sql(
      """
        |select CustomerId, StockCode, sum(Quantity)
        |from dfNoNullTable
        |group by CustomerId, StockCode
        |order by CustomerId desc, StockCode desc
      """.stripMargin).show()
    // or
    spark.sql(
      """
        |select CustomerId, StockCode, sum(Quantity)
        |from dfNoNullTable
        |group by CustomerId, StockCode grouping sets((CustomerId, StockCode))
        |order by CustomerId desc, StockCode desc
      """.stripMargin).show()

    spark.sql(
      """
        |select CustomerId, StockCode, sum(Quantity)
        |from dfNoNullTable
        |group by CustomerId, StockCode grouping sets((CustomerId, StockCode), ())
        |order by CustomerId desc, StockCode desc
      """.stripMargin).show()


    // rollup
    // This applies to the situation that there are specific hierarchical relations among columns.
    // E.g. country, state, city, suburb, street.
    dfNoNull.rollup("Date", "Country")
      .agg(sum("Quantity"))
      .orderBy("Date")
      .show(false)

    // cube
    dfNoNull.cube("Date", "Country")
      .agg(sum("Quantity"))
      .orderBy("Date")
      .show(false)









  }

}
