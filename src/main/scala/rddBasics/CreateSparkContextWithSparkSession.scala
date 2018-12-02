package rddBasics

import org.apache.spark.sql.SparkSession

object CreateSparkContextWithSparkSession {
  
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder()
        .appName("Create Spark Context with Spark Session")
        .master("local")
        .getOrCreate()
    val sc = spark.sparkContext
        
    val array = Array(1,2,3,4,5)
    val arrayRDD = sc.parallelize(array, 2)
    arrayRDD.foreach(println)
    
    val file = "src/main/resources/datasets/people.txt"
    val fileRDD = sc.textFile(file)
    println(fileRDD.count())  // count number of lines
    fileRDD.take(3).foreach(println)
    
  }
}