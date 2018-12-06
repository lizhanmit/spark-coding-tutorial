package datasetBasics

import org.apache.spark.sql.SparkSession

object DatasetOperations extends App {
  
  val spark = SparkSession.builder()
    .appName("Dataset operations")
    .master("local")
    .getOrCreate()
  
  // import this when creating a Dataset  
  import spark.implicits._
  
  val peopleDS = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/datasets/people_2.csv")
    .as[People]
  
  // get Dataset with conditions
  val filterDS = peopleDS.filter(peopleObj => peopleObj.job == "Developer")
  filterDS.show()
  println("Number of developers: " + filterDS.count())
  // or
  val whereDS = peopleDS.where(peopleDS("job") === "Developer")
  // or 
  // val whereDS = peopleDS.where("job == 'Developer'")
  whereDS.show()
  
  // this case class is only accessible in this object
  case class NameSaraly(name: String, salary: Double)
  // NOTE here, "select" statement returns a DataFrame rather than a Dataset 
  // so, you need to create a case class to convert it to a Dataset
  val selectDS = peopleDS.select("name", "salary").as[NameSaraly]
  selectDS.show()
  
}