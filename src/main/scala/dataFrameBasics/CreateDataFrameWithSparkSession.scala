package dataFrameBasics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row


object CreateDataFrameWithSparkSession {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Create a DataFrame using Spark Session")
      .master("local")
      .getOrCreate()
      
    val rdd = spark.sparkContext.parallelize(Array("a", "b", "c", "d"))
    val rowRDD = rdd.map(element => Row(element))
    
    val schema = StructType(
        // StructField(column name, column type, nullable)
        // :: Nil is used to construct a list
        StructField("I am a String", StringType, true) :: Nil  
    )

    val df = spark.createDataFrame(rowRDD, schema)
    df.printSchema()
    df.show(3)
    
  }
}