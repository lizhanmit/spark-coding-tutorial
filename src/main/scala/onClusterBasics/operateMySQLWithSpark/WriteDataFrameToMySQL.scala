package onClusterBasics.operateMySQLWithSpark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import java.util.Properties

/**
 * add Hadoop xml config files under resources folder:
 * core-site.xml
 * hdfs-site.xml
 * hive-site.xml
 * mapred-site.xml
 * yarn-site.xml
 * 
 * add "mysql-connector-java" maven dependency in pom.xml 
 */
object WriteDataFrameToMySQL extends App {
  val spark = SparkSession.builder()
    .appName("Create DataFrame from CSV file and then write to MySQL")
    .master("yarn")
    .config("spark.yarn.jars", "hdfs://<ipAddress>:<port>/<yourDirectory>/jars/*.jar")  // switch to your real directory
    .getOrCreate()
    
  val url = "jdbc:mysql://<ipAddress>:3306"
  val properties = new Properties()
  properties.put("user", "<yourUserName>")
  properties.put("password", "<yourPwd>")
  
  Class.forName("com.mysql.jdbc.Driver")
  
  val peopleDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/<yourDirectory>/people.csv")
    
  val table = "<databaseName>.people"

  peopleDF.write
    .mode(SaveMode.Overwrite)  // overwrite when data or table already exists
    .jdbc(url, table, properties)
}