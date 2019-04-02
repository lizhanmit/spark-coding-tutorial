package book.sparkDefiniteGuide.lowLevelApi

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}

object SharedVariables {
  case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: BigInt)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("sharedVariables")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    /*
     * broadcast variables
     */
    println("=== broadcast variables ===")
    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ")
    val wordsRDD = spark.sparkContext.parallelize(myCollection, 2)
    val supplementalData = Map("Spark" -> 1000, "Definitive" -> 200, "Big" -> -300, "Simple" -> 100)

    // create a broadcast variable
    val suppBroadcast = spark.sparkContext.broadcast(supplementalData)
    // access the broadcast variable
    suppBroadcast.value

    wordsRDD.map(word => (word, suppBroadcast.value.getOrElse(word, 0))).sortBy(_._2).collect().foreach(println)


    /*
     * accumulators
     */
    println("=== accumulators ===")
    val flightsDS = spark.read.format("parquet")
      .load("src/main/resources/sparkDefiniteGuide/inputData/flight-data/2010-summary.parquet")
      .as[Flight]

    val accChina = spark.sparkContext.longAccumulator("China")
    // or
    //val accChina = new LongAccumulator
    //spark.sparkContext.register(accChina, "China")

    // use accChina to count the number of flights to or from China
    def accChinaFunc(flight: Flight) = {
      if (flight.DEST_COUNTRY_NAME == "China" || flight.ORIGIN_COUNTRY_NAME == "China") {
        accChina.add(flight.count.toLong)
      }
    }

    // here use foreach() because it is an action
    // Spark can provide guarantees that perform only inside of actions
    flightsDS.foreach(accChinaFunc(_))
    println("Number of flights to or from China: " + accChina.value)


    // custom accumulators
    val accEven = new EvenAccumulator
    spark.sparkContext.register(accEven, "EvenAcc")
    flightsDS.foreach(flight => accEven.add(flight.count))
    println(accEven.value)
  }

  // only add even numbers to the accumulator
  class EvenAccumulator extends AccumulatorV2[BigInt, BigInt] {
    private var num: BigInt = 0

    override def isZero: Boolean = {
      this.num == 0
    }

    override def copy(): AccumulatorV2[BigInt, BigInt] = {
      new EvenAccumulator
    }

    override def reset(): Unit = {
      this.num = 0
    }

    override def add(v: BigInt): Unit = {
      if (v % 2 == 0) {
        this.num += v
      }
    }

    override def merge(other: AccumulatorV2[BigInt, BigInt]): Unit = {
      this.num += other.value
    }

    override def value: BigInt = {
      this.num
    }
  }
}
