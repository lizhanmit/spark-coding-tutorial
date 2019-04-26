package book.sparkDefiniteGuide.lowLevelApi


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object RDDs {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RDDs")
      .master("local[*]")
      .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", 5)

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._


    /*
     * interoperation between DataFrame, Datasets, and RDDs
     */
    // convert a Dataset[Long] to RDD[Long]
    spark.range(10).rdd
    // convert a Dataset[Long] to DataFrame, then to RDD[Long]
    spark.range(10).toDF().rdd.map(_.getLong(0))
    // convert a Dataset[Long] to RDD[Long], then to DataFrame
    spark.range(10).rdd.toDF()


    /*
     * create RDDs from a local collection
     */
    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ")
    val wordsRDD = spark.sparkContext.parallelize(myCollection, 2) // 2 is he number of partitions
    // you can name this RDD to show up in the Spark UI
    wordsRDD.setName("myWords")


    /*
     * create RDDs from data sources
     */
    // read a text file line by line
    // each line is a record in RDD
    spark.sparkContext.textFile("/some/path/withTextFiles")
    // read a text file as a whole
    // the file is a single record
    // e.g. when the file consists of a large JSON object
    spark.sparkContext.wholeTextFiles("/some/path/withTextFiles")


    /*
     * distinct
     */
    wordsRDD.distinct().count()


    /*
     * filter
     */
    def startsWithS(s: String) = {
      s.startsWith("S")
    }

    wordsRDD.filter(startsWithS(_)).collect()


    /*
     * map
     */
    val wordsRDD2 = wordsRDD.map(word => (word, word(0), word.startsWith("S")))
    wordsRDD2.filter(_._3).take(5)


    /*
     * flatMap
     */
    wordsRDD.flatMap(_.toSeq).take(5)


    /*
     * sort
     */
    // sort by length of word from longest to shortest
    wordsRDD.sortBy(_.length * -1).take(5)


    /*
     * randomSplit
     * randomly split an RDD into an Array of RDDs
     */
    val fiftyFiftySplit = wordsRDD.randomSplit(Array[Double](0.5, 0.5))


    /*
     * reduce
     */
    spark.sparkContext.parallelize(1 to 20).reduce(_ + _)

    // find the longest word
    def wordLengthReducer(word1: String, word2: String): String = {
      if (word1.length > word2.length) {
        return word1
      }
      return word2
    }

    wordsRDD.reduce(wordLengthReducer)


    /*
     * countApprox
     */
    val confidence = 0.95
    val timeoutMilliseconds = 400
    wordsRDD.countApprox(timeoutMilliseconds, confidence)


    /*
     * countByValue
     * This method loads the result set into the memory of the driver.
     * Use this method only if the  resulting map is expected to be small,
     * such as the total number of rows is low or the number of distinct items is low.
     */
    wordsRDD.countByValue()


    /*
     * countByValueApprox
     */
    val confidence2 = 0.9
    val timeoutMilliseconds2 = 1000
    wordsRDD.countByValueApprox(timeoutMilliseconds2, confidence2)


    /*
     * take
     */
    wordsRDD.take(5)

    wordsRDD.top(5)
    // opposite
    wordsRDD.takeOrdered(5)

    val withReplacement = true
    val numberToTake = 5
    val randomSeed = 100L
    wordsRDD.takeSample(withReplacement, numberToTake, randomSeed)


    /*
     * saving files
     */
    // saving as plain-text files
    wordsRDD.saveAsTextFile("file:/tmp/bookTitle")
    // optionally with a compression codec
    import org.apache.hadoop.io.compress.BZip2Codec
    wordsRDD.saveAsTextFile("file:/tmp/bookTitle", classOf[BZip2Codec])

    // saving as SequenceFiles which consist of binary key-value paris
    wordsRDD.saveAsObjectFile("/tmp/my/sequenceFilePath")


    /*
     * caching
     */
    wordsRDD.cache()

    wordsRDD.getStorageLevel


    /*
     * checkpointing
     */
    spark.sparkContext.setCheckpointDir("/some/path/for/checkpointing")
    // cache the RDD on disk for future reference
    wordsRDD.checkpoint()


    /*
     * pipe
     * pipe RDDs to system commands
     */
    wordsRDD.pipe("wc -l").collect()


    /*
     * mapPartitions
     */
    // create the value “1” for every partition
    // then sum to count the number of partitions
    wordsRDD.mapPartitions(part => Iterator[Int](1)).sum()

    def indexedFunc(partitionIndex: Int, withinPartIterator: Iterator[String]) = {
      withinPartIterator.toList.map(value => s"Partition: $partitionIndex - $value").iterator
    }
    wordsRDD.mapPartitionsWithIndex(indexedFunc).collect()


    /*
     * foreachPartition
     */
    import java.io._
    import scala.util.Random
    wordsRDD.foreachPartition(iter => {
      val randomFileName = new Random().nextInt()
      val pw = new PrintWriter(new File(s"/tmp/random-file-${randomFileName}.txt"))
      while (iter.hasNext) {
        pw.write(iter.next())
      }
      pw.close()
    })
  }
}
