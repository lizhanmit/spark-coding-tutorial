package book.sparkDefiniteGuide.lowLevelApi

import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession
import scala.util.Random

object AdvancedRDDs {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AdvancedRDDs")
      .master("local[*]")
      .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", 5)

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ")
    val wordsRDD = spark.sparkContext.parallelize(myCollection, 2)

    wordsRDD.map(word => (word.toLowerCase, 1))

    /*
     * key-value pair RDDs
     */
    // keyBy: used to create a key for each record
    val keywordRDD = wordsRDD.keyBy(_.toSeq(0).toLower.toString)
    keywordRDD.collect().foreach(println)

    // mapValues
    keywordRDD.mapValues(_.toUpperCase()).collect().foreach(println)

    // flatMapValues
    // string in each record will become characters
    keywordRDD.flatMapValues(_.toUpperCase())

    // extract keys and values
    keywordRDD.keys.collect().foreach(println)
    keywordRDD.values.collect().foreach(println)

    // lookup
    // search records with "s" as key
    keywordRDD.lookup("s")

    // sampleByKey
    val distinctChars = wordsRDD.flatMap(_.toLowerCase().toSeq).distinct().collect()
    val sampleMap = distinctChars.map(c => (c, new Random().nextDouble())).toMap
    wordsRDD.map(word => (word.toSeq(0).toLower, word)).sampleByKey(true, sampleMap, 6L).collect()

    // sampleByKeyExact
    wordsRDD.map(word => (word.toSeq(0).toLower, word)).sampleByKeyExact(true, sampleMap, 6L).collect()


    /*
     * aggregations
     */
    val chars = wordsRDD.flatMap(_.toLowerCase().toSeq)
    val kvChars = chars.map((_, 1))
    def maxFunc(left: Int, right: Int) = math.max(left, right)
    def addFunc(left: Int, right: Int) = left + right

    // countByKey
    kvChars.countByKey()
    // countByKeyApprox
    val timeout = 1000L
    val confidence = 0.95
    kvChars.countByKeyApprox(timeout, confidence)

    // groupByKey + reduce
    kvChars.groupByKey().map(row => (row._1, row._2.reduce(addFunc))).collect()
    // reduceByKey (preferrable)
    kvChars.reduceByKey(addFunc).collect()

    // foldByKey
    kvChars.foldByKey(0)(addFunc).collect()


    /*
     * cogroup
     */
    val distinctCharsRDD = wordsRDD.flatMap(_.toLowerCase().toSeq).distinct()
    val charRDD = distinctCharsRDD.map((_, new Random().nextDouble()))
    val charRDD2 = distinctCharsRDD.map((_, new Random().nextDouble()))
    val charRDD3 = distinctCharsRDD.map((_, new Random().nextDouble()))
    charRDD.cogroup(charRDD2, charRDD3).collect()


    /*
     * join
     */
    kvChars.join(charRDD).collect()
    // here, 10 is the number of output partitions
    kvChars.join(charRDD, 10).collect()


    /*
     * zip
     * create a pairRDD
     * two RDDs must have the same number of partitions and records
     */
    val numRange = spark.sparkContext.parallelize(0 to 9, 2)
    wordsRDD.zip(numRange).collect()


    /*
     * custom partitioning
     */
    val df = spark.read
      .format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load("src/main/resources/sparkDefiniteGuide/inputData/retail-data/all")

    df.printSchema()

    val rdd = df.coalesce(10).rdd
    // r(6): get field "CustomerID"
    rdd.map(r => r(6)).take(5).foreach(println)
    val keyedRDD = rdd.keyBy(r => r(6).asInstanceOf[Int].toDouble)
    keyedRDD.partitionBy(new HashPartitioner(10)).take(10).foreach(println)


    // custom partitioning
    // glom(): convert each partition to an array, which consists of customer ids
    // here, the result is count of distinct customer id in each partition
    // three partitons here, the count for the first one should be 2
    keyedRDD.partitionBy(new DomainPartitioner).map(_._1).glom().map(_.toSet.size).take(5).foreach(println)
  }

  import org.apache.spark.Partitioner
  class DomainPartitioner extends Partitioner {

    override def numPartitions: Int = 3

    override def getPartition(key: Any): Int = {
      val customerId = key.asInstanceOf[Double].toInt
      if (customerId == 17850 || customerId == 12583) {
        return 0
      }

      // return 1 or 2 as partition No.
      new Random().nextInt(2) + 1
    }
  }
}
