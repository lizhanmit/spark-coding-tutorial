package onClusterBasics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

/**
 * add Hadoop xml config files under resources folder:
 * core-site.xml
 * hdfs-site.xml
 * hive-site.xml
 * mapred-site.xml
 * yarn-site.xml
 */
object WriteDataFrameToHive extends App {
  val spark = SparkSession.builder()
    .appName("Create DataFrame from CSV file and then write to Hive")
    .master("yarn")
    .config("spark.yarn.jars", "hdfs://<ipAddress>:<port>/<yourDirectory>/jars/*.jar")  // switch to your real directory
    .enableHiveSupport()  // add this when you are using Hive
    .getOrCreate()
    
  val peopleDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/<yourDirectory>/people.csv")
    
  peopleDF.write
    .mode(SaveMode.Overwrite)  // overwrite when data or table already exists
    .saveAsTable("<databaseName>.people")
    
  // if the Hive table already exists, 
  // you can use .insertInto() method to insert data into the table
  // NOTE: the order of fields in the DataFrame and Hive table should be the same (you can adjust the DataFrame) 
  // otherwise the original order of fields in Hive table will be overwrote by DataFrame
  peopleDF.write
    .mode(SaveMode.Overwrite)
    .insertInto("<databaseName>.people")  
}