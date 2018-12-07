package onClusterBasics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType

/**
 * add Hadoop xml config files under resources folder:
 * core-site.xml
 * hdfs-site.xml
 * hive-site.xml
 * mapred-site.xml
 * yarn-site.xml
 * 
 * 1. Read a CSV file as a DataFrame
 * 2. Create a Hive table using Catalog API
 * 3. Insert the DataFrame into the Hive table
 */
object CreateHiveTableWithCatalogAPI extends App {
  val spark = SparkSession.builder()
    .appName("Create Hive table using Catalog API")
    .master("yarn")
    .config("spark.yarn.jars", "hdfs://<ipAddress>:<port>/<yourDirectory>/jars/*.jar")  // switch to your real directory
    .enableHiveSupport()  // add this when you are using Hive
    .getOrCreate()
    
  val peopleDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/<yourDirectory>/people.csv")
  
  // schema for the above DataFrame  
  val schema = StructType(
      StructField("name", StringType, true) :: 
      StructField("age", IntegerType, true) ::
      StructField("job", StringType, true) :: Nil
  )
    
  spark.catalog.createTable("<databaseName>.people", "parquet", schema, Map("Comment" -> "Create table using Catalog API"))
  println("If the table was created successfully: " + spark.catalog.tableExists("<databaseName>.people"))
  
  peopleDF.write.insertInto("<databaseName>.people")
  spark.table("<databaseName>.people").show()
}