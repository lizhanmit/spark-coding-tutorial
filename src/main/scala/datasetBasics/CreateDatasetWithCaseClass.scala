package datasetBasics

import org.apache.spark.sql.SparkSession

// this case class is accessible in its package
case class People(name: String, age: Integer, job: String, salary: Float) 

case class Sales(region: String, 
                  country: String, 
                  itemType: String, 
                  salesChannel: String, 
                  orderPriority: String, 
                  orderDate: String, 
                  orderId: Long, 
                  shipDate: String, 
                  unitsSold: Integer, 
                  unitPrice: Double,
                  unitCost: Double,
                  totalRevenue: Double, 
                  totalCost: Double, 
                  totalProfit: Double)
                  
object CreateDatasetWithCaseClass extends App {
  
  val spark = SparkSession.builder()
    .appName("Create a Dataset with a case class")
    .master("local")
    .getOrCreate()
  
  // import this when creating a Dataset
  import spark.implicits._
    
  val peopleDS = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/datasets/people_2.csv")
    .as[People]
  
  peopleDS.show(5)
  
  
  /*
   * for situation that header of CSV file does not match fields of the case class 
   * need to do some adjustments when after reading the file
   */
  val salesDF = spark.read 
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/datasets/10000 Sales Records.csv")
    // do not continue to .as[Sales] here 
  
  // modify the header of CSV file to make them the same as the fields of the case class
  // if there is no space, lowercase it
  // if there are spaces, remove the spaces and apply camel case
  val columnNames = salesDF.columns.map(
      col => {
         if (col.contains(" ")) {
           val a = col.split(" ")
           a(0).toLowerCase() + a(1).capitalize
         } else {
           col.toLowerCase()
         }
      } 
  )
  val salesDFWithAjustedColumnNames = salesDF.toDF(columnNames: _*)  // convert array of String to varargs
  
  val salesDS = salesDFWithAjustedColumnNames.as[Sales]
  salesDS.printSchema()
  salesDS.show(5)
}