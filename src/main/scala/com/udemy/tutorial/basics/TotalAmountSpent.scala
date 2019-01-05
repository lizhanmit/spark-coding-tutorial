package com.udemy.tutorial.basics

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

object TotalAmountSpent {
  def main(args: Array[String]): Unit = {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
     
    val spark = SparkSession
      .builder()
      .appName("Total Amount Spent by Customer")
      .master("local")
      .getOrCreate()
      
    val file = "src/main/resources/udemy/inputData/customer-orders.csv"
    val lines = spark.sparkContext.textFile(file)
    val rdd = lines.map(parseLine)
    val amountByCustomerId = rdd.reduceByKey(_ + _)
    val results = amountByCustomerId.collect()
    // sort by amount
    results.sortBy(x => x._2).foreach(println)
  }
  
  def parseLine(line: String) = {
    val fields = line.split(",")
    val customerId = fields(0).toInt
    val amount = fields(2).toFloat
    (customerId, amount)
  }
  
}