package onClusterBasics.operateMySQLWithSpark

import org.apache.spark.sql.SparkSession
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
object CreateDataFrameWithMySQL extends App {
  val spark = SparkSession.builder()
    .appName("Create DataFrame with MySQL")
    .master("yarn")
    .config("spark.yarn.jars", "hdfs://<ipAddress>:<port>/<yourDirectory>/jars/*.jar")  // switch to your real directory
    .getOrCreate()
    
  val url = "jdbc:mysql://<ipAddress>:3306"
  val table = "<databaseName>.people"
  val properties = new Properties()
  properties.put("user", "<yourUserName>")
  properties.put("password", "<yourPwd>")
  
  Class.forName("com.mysql.jdbc.Driver")
  
  val mysqlDF = spark.read.jdbc(url, table, properties)
  mysqlDF.show()
  
  // group the DF by different jobs and count them
  val jobCountMysqlDF = mysqlDF.select("job").groupBy("job").count()
  jobCountMysqlDF.show()
  
  // or using sql statement
  val query = "select job, count(*) as jobCount from <databaseName>.people group by job"
  val queryResultDF = spark.read.jdbc(url, s"($query) as job_count_table", properties)
  queryResultDF.show()
}