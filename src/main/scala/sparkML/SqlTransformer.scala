package sparkML

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.SQLTransformer
import org.apache.log4j._

object SqlTransformer {
  def main(args: Array[String]): Unit = {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession.builder()
      .appName("SQL Transformer")
      .master("local")
      .getOrCreate()
      
    val sales = spark.read.format("csv")
                          .option("header", "true")
                          .option("inferSchema", "true")
                          .load("src/main/resources/datasets/2010-12-01.csv")
                          .where("Description IS NOT NULL")
                          
    //sales.show()
    
    val basicTransformation = new SQLTransformer()
                                  .setStatement("""
                                      SELECT sum(Quantity), count(*), CustomerID
                                      FROM __THIS__
                                      GROUP BY CustomerID
                                    """)

    basicTransformation.transform(sales).show()
  }
}