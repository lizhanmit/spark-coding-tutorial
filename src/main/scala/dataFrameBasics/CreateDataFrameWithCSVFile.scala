package dataFrameBasics

import org.apache.spark.sql.SparkSession

object CreateDataFrameWithCSVFile {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Create a DataFrame with a CSV file using Spark Session")
      .master("local")
      .getOrCreate()
      
    val df = spark.read
      // if the csv file contains header
      // if you do not write this, the header will also be included in the DataFrame, which is not wanted 
      .option("header", "true")  
      .option("inferSchema", "true")  // if you want to infer schema of the csv file
      // or
      // .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv("src/main/resources/datasets/people.csv")
    df.printSchema()
    df.show()
  }
}