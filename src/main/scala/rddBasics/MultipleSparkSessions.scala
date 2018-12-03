package rddBasics

import org.apache.spark.sql.SparkSession

/**
 * Spark 1.x does not support multiple SparkSessions in one job
 * Spark 2.x supports it
 */
object MultipleSparkSessions {
  def main(args: Array[String]): Unit = {
    val spark1 = SparkSession.builder()
      .appName("Create multiple Spark Sessions 1")
      .master("local")
      .getOrCreate()
      
    val spark2 = SparkSession.builder()
      .appName("Create multiple Spark Sessions 2")
      .master("local")
      .getOrCreate()
      
    val rdd1 = spark1.sparkContext.parallelize(Array(1,2,3))
    rdd1.take(3).foreach(println)
    
    val rdd2 = spark2.sparkContext.parallelize(Array(4,5,6))
    rdd2.take(3).foreach(println)
    
  }
}