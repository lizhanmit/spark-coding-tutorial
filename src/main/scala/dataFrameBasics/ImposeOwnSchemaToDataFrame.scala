package dataFrameBasics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.FloatType

object ImposeOwnSchemaToDataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Impose own schema to a DataFrame")
      .master("local")
      .getOrCreate()
      
    val peopleDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/datasets/people_2.csv")
      
    println("default schema")
    peopleDF.printSchema()
    
    // by default, column "salary" is inferred as integer type
    // now we impose it as float type
    val ownSchema = StructType(
        // StructField("<columnName>", <columnType>, <nullable>)
        StructField("name", StringType, true) ::
        StructField("age", IntegerType, true) ::
        StructField("job", StringType, true) ::
        StructField("salary", FloatType, true) :: Nil  // use :: Nil to construct a list
    )
    
    val peopleDFWithOwnSchema = spark.read
      .option("header", "true")
      .schema(ownSchema)
      .csv("src/main/resources/datasets/people_2.csv")  // beside csv files, other formats are applicable
      
    println("own schema")
    peopleDFWithOwnSchema.printSchema()
    
  }
}