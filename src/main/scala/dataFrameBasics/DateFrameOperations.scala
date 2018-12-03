package dataFrameBasics

import org.apache.spark.sql.SparkSession

object DateFrameOperations {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrame operations")
      .master("local")
      .getOrCreate()
      
    val usersDF = spark.read.orc("src/main/resources/datasets/users.orc")

    val usersSchema = usersDF.schema
    println(usersSchema)
    
    val columnNames = usersDF.columns
    println(columnNames.mkString(", "))
    
    // describe a column
    usersDF.describe("name").show()
    
    // get column names and their data types
    usersDF.dtypes.foreach(println)
    
    usersDF.head(3).foreach(println)
    
    // select specific columns as a new DataFrame
    usersDF.select("name", "favorite_color").show(5)
    
    // get DataFrame with conditions (similar with sql statement)
    usersDF.where("favorite_color == 'red'").show(5)
    // or 
    // usersDF.filter(usersDF("favorite_color") === "red").show(5)
    
    
    val users2DF = spark.read.orc("src/main/resources/datasets/users_2.orc")
    users2DF.groupBy("favorite_color").count().show()

  }
}