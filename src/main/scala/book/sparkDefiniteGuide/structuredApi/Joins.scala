package book.sparkDefiniteGuide.structuredApi

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Joins {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Joins")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    val personDF = Seq(
      (0, "Bill Chambers", 0, Seq(100)),
      (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
      (2, "Michael Armbrust", 1, Seq(250, 100)))
      .toDF("id", "name", "graduate_program", "spark_status")
    val graduateProgramDF = Seq(
      (0, "Masters", "School of Information", "UC Berkeley"),
      (2, "Masters", "EECS", "UC Berkeley"),
      (1, "Ph.D.", "EECS", "UC Berkeley"))
      .toDF("id", "degree", "department", "school")
    val sparkStatusDF = Seq(
      (500, "Vice President"),
      (250, "PMC Member"),
      (100, "Contributor"))
      .toDF("id", "status")

    personDF.createOrReplaceTempView("personTable")
    graduateProgramDF.createOrReplaceTempView("graduateProgramTable")
    sparkStatusDF.createOrReplaceTempView("sparkStatusTable")


    /*
     * inner join (default join)
     */
    println("=== inner join ===")
    personDF.join(graduateProgramDF, personDF.col("graduate_program") === graduateProgramDF.col("id")).show(3)
    // or
    spark.sql(
      """
        |select *
        |from personTable join graduateProgramTable on personTable.graduate_program = graduateProgramTable.id
        |limit 3
      """.stripMargin).show()
    // or
    personDF.join(graduateProgramDF, personDF.col("graduate_program") === graduateProgramDF.col("id"), "inner").show(3)
    // or
    spark.sql(
      """
        |select *
        |from personTable inner join graduateProgramTable on personTable.graduate_program = graduateProgramTable.id
        |limit 3
      """.stripMargin).show()


    // other join types:
    // for DataFrame, "outer", "left_outer", "right_outer", "left_semi", "left_anti", "cross"
    // for Spark SQL, outer join, left outer join, right outer join, left semi join, left anti join, cross join


    /*
     * joins on complex types
     */
    println("=== joins on complex types ===")
    personDF.withColumnRenamed("id", "personId")
        .join(sparkStatusDF, expr("array_contains(spark_status, id)"))
        .show()
    // or
    spark.sql(
      """
        |select *
        |from (select id as personId, name, graduate_program, spark_status from personTable)
        |inner join sparkStatusTable
        |on array_contains(spark_status, id)
      """.stripMargin).show()


    /*
     * handling duplicate column names
     */
    println("=== handling duplicate column names ===")
    val graduateProgramDupeDF = graduateProgramDF.withColumnRenamed("id", "graduate_program")
    graduateProgramDupeDF.join(personDF, graduateProgramDupeDF.col("graduate_program") === personDF.col("graduate_program"))
      .show()  // there will be two "graduate_program" columns in the result
               // if you select this column based on it, you will get ambiguous errors

    // approach 1: change the join expression from a Boolean expression to a string or sequence
    // this will remove one duplicate column
    // now there is only one "graduate_program" column in the result
    println("approach 1:")
    graduateProgramDupeDF.join(personDF, "graduate_program").show()

    // approach 2: drop one duplicate column after the join
    println("approach 2:")
    graduateProgramDupeDF.join(personDF, graduateProgramDupeDF.col("graduate_program") === personDF.col("graduate_program"))
      .drop(graduateProgramDupeDF.col("graduate_program"))
      .show()

    // approach 3: rename a column before the join


    /*
     * broadcast joins
     */
    personDF.join(broadcast(graduateProgramDF), personDF.col("graduate_program") === graduateProgramDF.col("id")).explain()

    spark.sql(
      """
        |select /*+ MAPJOIN(graduateProgramTable) */ *
        |from personTable join graduateProgramTable
        |on personTable.graduate_program = graduateProgramTable.id
      """.stripMargin).show()
    // here MAPJOIN can be substituted by BROADCAST or BROADCASTJOIN
  }
}
