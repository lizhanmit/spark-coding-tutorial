package sparkML

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.MaxAbsScaler
import org.apache.log4j._

object StandardScaler {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession.builder()
      .appName("StandardScaler")
      .master("local")
      .getOrCreate()
      
    val scaleDF = spark.read.parquet("src/main/resources/datasets/simple-ml-scaling")
    scaleDF.show()
    
    val maScaler = new MaxAbsScaler().setInputCol("features")
    val fittedmaScaler = maScaler.fit(scaleDF)
    fittedmaScaler.transform(scaleDF).show()
  
  }
}