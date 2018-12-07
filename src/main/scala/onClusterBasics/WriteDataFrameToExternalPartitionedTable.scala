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
object WriteDataFrameToExternalPartitionedTable extends App {
  val spark = SparkSession.builder()
    .appName("Create DataFrame from CSV file and then write to external partitioned Hive tables")
    .master("yarn")
    .config("spark.yarn.jars", "hdfs://<ipAddress>:<port>/<yourDirectory>/jars/*.jar")  // switch to your real directory
    .enableHiveSupport()  // add this when you are using Hive
    .getOrCreate()
    
  val peopleDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/<yourDirectory>/people.csv")
    
  peopleDF.write
    .partitionBy("job")  // partition by "job" field
    .mode(SaveMode.Overwrite)  // overwrite when data or table already exists
    .option("path", "<destinationDirectory>")  // specify destination directory of the external table
    .saveAsTable("<databaseName>.externalPartitionedPeople")
}