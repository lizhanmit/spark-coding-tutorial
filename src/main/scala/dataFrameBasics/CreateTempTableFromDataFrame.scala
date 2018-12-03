package dataFrameBasics

import org.apache.spark.sql.SparkSession

object CreateTempTableFromDataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Create a temp table from DataFrame")
      .master("local")
      .getOrCreate()
      
    val users2DF = spark.read.orc("src/main/resources/datasets/users_2.orc")
    
    users2DF.createTempView("users2_table")  
    val subUsers2DF = spark.sql("select * from users2_table where favorite_color = 'red'")
    subUsers2DF.show()
    
    subUsers2DF.createOrReplaceTempView("users2_table")  // this will replace the existing "users2_table"
    spark.sql("select * from users2_table limit 2").show()
  }
}