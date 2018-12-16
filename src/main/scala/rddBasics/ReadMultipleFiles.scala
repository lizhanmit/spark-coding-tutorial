package rddBasics

import org.apache.spark.sql.SparkSession
import org.apache.log4j._

object ReadMultipleFiles {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    
    val spark = SparkSession.builder()
        .appName("Create RDDs with CSV files")
        .master("local")
        .getOrCreate()
        
    val multipleFiles = "src/main/resources/datasets/*.txt"
    val multipleFilesRDD = spark.sparkContext.textFile(multipleFiles)
    multipleFilesRDD.collect().foreach(println)
  }
}