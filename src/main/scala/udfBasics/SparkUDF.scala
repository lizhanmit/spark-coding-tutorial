package udfBasics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object SparkUDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark UDF")
      .master("local")
      .getOrCreate()
      
    val peopleDF = spark.read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv("src/main/resources/datasets/people_2.csv")
      
    peopleDF.select(peopleDF.col("name")).show()
    
    // 2. Convert the function into a UDF
    // udf[<returnType, inputParameterType>](<function>)
    val myToUpperCaseUDF = udf[String, String](myToUpperCaseFunc) 
    val myCalculateSuperUDF = udf[Double, Double](myCalculateSuperFunc)
    
    // 3. Apply the UDF to DataFrame or Dataset
    peopleDF.select(myToUpperCaseUDF(peopleDF.col("name"))).show()
    
    // another way to apply UDF: with Spark SQL 
    spark.sqlContext.udf.register("registeredUDF", myCalculateSuperUDF)
    peopleDF.createOrReplaceTempView("peopleTable")
    spark.sql("select *, registeredUDF(salary) as super from peopleTable").show()
  }
  
  // 1. Define a function
  def myToUpperCaseFunc(s: String): String = s.toUpperCase()
  def myCalculateSuperFunc(salary: Double): Double = salary * 9.5 / 100;
}