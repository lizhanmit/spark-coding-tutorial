package onClusterBasics

import org.apache.spark.sql.SparkSession

/**
 * add Hadoop xml config files under resources folder:
 * core-site.xml
 * hdfs-site.xml
 * hive-site.xml
 * mapred-site.xml
 * yarn-site.xml
 */
object CatalogAPI extends App {
  val spark = SparkSession.builder()
    .appName("Catalog API")
    .master("yarn")
    .config("spark.yarn.jars", "hdfs://<ipAddress>:<port>/<yourDirectory>/jars/*.jar")  // switch to your real directory
    .enableHiveSupport()  // add this when you are using Hive
    .getOrCreate()
    
  val catalog = spark.catalog
  
  println(catalog.currentDatabase)
  
  catalog.setCurrentDatabase("<databaseName>")
  
  // show info about all tables in the current database
  // NOTE: this is a Dataset
  catalog.listTables().show()
}