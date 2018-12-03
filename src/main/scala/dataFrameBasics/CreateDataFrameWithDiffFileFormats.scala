package dataFrameBasics

import org.apache.spark.sql.SparkSession

/**
 * Spark 1.x does not support csv file
 * Spark 2.x support it 
 */
object CreateDataFrameWithDiffFileFormats {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Create DataFrames with different file formats")
      .master("local")
      .getOrCreate()
      
    val jsonDF = spark.read.json("src/main/resources/datasets/people.json")
    println("json format")
    jsonDF.printSchema()
    jsonDF.show(5)
    
    val orcDF = spark.read.orc("src/main/resources/datasets/users.orc")
    println("arc format")
    orcDF.printSchema()
    orcDF.show(5)
    
    val parquetDF = spark.read.parquet("src/main/resources/datasets/users.parquet")
    println("parquet format")
    parquetDF.printSchema()
    parquetDF.show(5)
  }
}