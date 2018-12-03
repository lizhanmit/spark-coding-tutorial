package dataFrameBasics

import org.apache.spark.sql.SparkSession

/**
 * Spark does not support Avro file out of box 
 * you need to add an additional dependency in pom.xml
 * otherwise, org.apache.spark.sql.AnalysisException: Failed to find data source: avro.
 */
object CreateDataFrameWithAvroFile {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Create a DataFrame with Avro file")
      .master("local")
      .getOrCreate()
      
    val avroDF = spark.read.format("avro").load("src/main/resources/datasets/users.avro")
    avroDF.printSchema()
    avroDF.show(5)
    
    avroDF.write.format("avro").save("src/main/resources/output/usersOutputAvro")
  }
}