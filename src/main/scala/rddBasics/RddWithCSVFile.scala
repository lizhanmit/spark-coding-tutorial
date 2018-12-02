package rddBasics

import org.apache.spark.sql.SparkSession

object RddWithCSVFile {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("Create RDDs with CSV files")
        .master("local")
        .getOrCreate()
    val sc = spark.sparkContext    
    
    val file = "src/main/resources/datasets/people.csv"
    val csvRDD = sc.textFile(file)
    println(csvRDD.count)  // count number of lines
    csvRDD.take(3).foreach(println)
    
    val header = csvRDD.first()
    val csvRDDWithoutHeader = csvRDD.filter(_ != header)
    // or 
    // val csvRDDWithoutHeader = csvRDD.filter(line => line != header)
    csvRDDWithoutHeader.take(3).foreach(println)
    
    val peopleWithLimitedCols = csvRDDWithoutHeader.map(line => {
      val colArray = line.split(";")
      List(colArray(0), colArray(1))  // return as a list
      // (colArray(0), colArray(1))  // return as a tuple 
      // Array(colArray(0), colArray(1)).mkString(":")
    })
    peopleWithLimitedCols.take(3).foreach(println)
    
    // output directory should not be existed
    // otherwise, org.apache.hadoop.mapred.FileAlreadyExistsException
    // the directory here is a folder rather than a file
    // under this directory there will be some files, such as part-00000, part-00001, ...
    // the number of part-* files equals the number of partitions you specified when reading the source file
    peopleWithLimitedCols.saveAsTextFile("src/main/resources/output/peopleWithLimitedCols")

  }
}