package book.sparkDefiniteGuide.structuredApi

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object WorkWithDifferentTypesOfData {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("WorkWithDifferentTypesOfData")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    spark.conf.set("spark.sql.shuffle.partitions", "5")

    val file = "src/main/resources/sparkDefiniteGuide/inputData/retail-data/by-day/2010-12-01.csv"

    val df = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(file)

    df.printSchema()
    df.createOrReplaceTempView("dfTable")

    df.cache()


    df.select(lit(5), lit("five"), lit(5.0)).show(3)


    /*
     * working with booleans
     */
    println("=== working with booleans ===")
    df.where(col("InvoiceNo").equalTo(536365))
      .select("InvoiceNo", "Description").show(3, false)
    // or
    df.where(col("InvoiceNo") === 536365)
      .select("InvoiceNo", "Description").show(3, false)
    // or
    df.where("InvoiceNo = 536365")
      .select("InvoiceNo", "Description").show(3, false)
    // or
    spark.sql(
      """
        select InvoiceNo, Description
        from dfTable
        where InvoiceNo = 536365
        limit 3
      """).show(false)

    // does not equal
    df.where("InvoiceNo <> 536365").show(3, false)

    val priceFilter = col("UnitPrice") > 600
    val descripFilter = col("Description").contains("POSTAGE")
    df.where(col("StockCode").isin("DOT")).where(priceFilter.or(descripFilter)).show(3)
    // or
    // instr(string1, string2): get the index of string2 in string1, the index is 1-based
    // so if the return value >= 1, it means string1 contains string2
    spark.sql(
      """
        |select *
        |from dfTable
        |where StockCode in ("DOT") and (UnitPrice > 600 or instr(Description, "POSTAGE") >= 1)
        |limit 3
      """.stripMargin).show()

    // specify a Boolean column, a column with true or false value
    val dOTCodeFilter = col("StockCode")  === "DOT"
    df.withColumn("isExpensive", dOTCodeFilter.and(priceFilter.or(descripFilter)))
      .where("isExpensive")
      .select("UnitPrice", "isExpensive")
      .show(3)
    // or
    spark.sql(
      """
        |select UnitPrice, (StockCode = "DOT" and (UnitPrice > 600 or instr(Description, "POSTAGE") >= 1)) as isExpensive
        |from dfTable
        |where (StockCode = "DOT" and (UnitPrice > 600 or instr(Description, "POSTAGE") >= 1))
      """.stripMargin)


    df.withColumn("isExpensive", not(col("UnitPrice").leq(250)))
      .filter("isExpensive")
      .select("Description", "UnitPrice")
      .show(3)
    // or
    df.withColumn("isExpensive", expr("NOT UnitPrice <= 250"))
      .filter("isExpensive")
      .select("Description", "UnitPrice")
      .show(3)

    // if there is a null value in data
    // null-safe equivalence test
    df.where(col("Description").eqNullSafe("hello")).show(3)


    /*
     * working with numbers
     */
    println("=== working with numbers ===")
    val fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
    df.select(expr("CustomerID"), fabricatedQuantity.alias("realQuantity")).show(3)
    // or
    df.selectExpr("CustomerID", "(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity").show(3)
    // or
    spark.sql(
      """
        |select CustomerID, (POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity
        |from dfTable
        |limit 3
      """.stripMargin).show()

    // rounding
    df.select(round(lit(2.5)), bround(lit(2.5))).show(3)

    spark.sql(
      """
        |select round(2.5), bround(2.5)
      """.stripMargin).show()

    // Pearson correlation coefficient
    println(df.stat.corr("Quantity", "UnitPrice"))
    df.select(corr("Quantity", "UnitPrice")).show()
    // or
    spark.sql(
      """
        |select corr(Quantity, UnitPrice)
        |from dfTable
      """.stripMargin).show()

    // summary statistics
    df.describe().show()

    // generate a unique monotonically increasing id for each row
    df.select(monotonically_increasing_id()).show(3)


    /*
     * working with strings
     */
    println("=== working with strings ===")
    df.select(initcap(col("Description"))).show(3, false)
    // or
    spark.sql(
      """
        |select initcap(Description)
        |from dfTable
        |limit 3
      """.stripMargin).show(false)

    df.select(col("Description"), lower(col("Description"))).show(3, false)
    // or
    spark.sql(
      """
        |select Description, lower(Description)
        |from dfTable
        |limit 3
      """.stripMargin).show(false)

    // trim
    df.select(
      ltrim(lit("   hello   ")).as("ltrim"),
      rtrim(lit("   hello   ")).as("rtrim"),
      trim(lit("   hello   ")).as("trim"),
      lpad(lit("hello"), 3, " ").as("lpad"), // lpad(<column>, <length_of_final_string>, <padding_stuff>)
      rpad(lit("hello"), 10, " ").as("rpad")
    ).show(3)


    /*
     * regular expression
     */
    // regexp_replace
    println("=== regular expression ===")
    val simpleColors = Seq("black", "white", "red", "green", "blue")
    val regexString = simpleColors.map(_.toUpperCase).mkString("|")  // the | signifies `OR` in regular expression syntax
    df.select(regexp_replace(col("Description"), regexString, "COLOR").alias("color_clean"), col("Description")).show(3, false)
    // or
    spark.sql(
      """
        |select regexp_replace(Description, 'BLACK|WHITE|RED|GREEN|BLUE', 'COLOR') as color_clean, Description
        |from dfTable
        |limit 3
      """.stripMargin).show(false)

    // translate
    df.select(translate(col("Description"), "LEET", "1337"), col("Description")).show(2, false)
    // or
    spark.sql(
      """
        |select translate(Description, 'LEET', '1337'), Description
        |from dfTable
        |limit 3
      """.stripMargin).show(false)

    // regexp_extract
    val regexString2 = simpleColors.map(_.toUpperCase()).mkString("(", "|", ")")
    df.select(regexp_extract(col("Description"), regexString2, 1).alias("color_clean_2"), col("Description")).show(false)
    // or
    spark.sql(
      """
        |select regexp_extract(Description, '(BLACK|WHITE|RED|GREEN|BLUE)', 1) as color_clean_2, Description
        |from dfTable
      """.stripMargin).show(false)

    // contains
    val containsBlack = col("Description").contains("BLACK")
    val containsWhite = col("Description").contains("WHITE")
    df.withColumn("hasSimpleColor", containsBlack.or(containsWhite))
      .where(col("hasSimpleColor"))
      .select(col("Description"))
      .show(false)
    // or
    spark.sql(
      """
        |select Description
        |from dfTable
        |where instr(Description, 'BLACK') >= 1 or instr(Description, 'WHITE') >= 1
      """.stripMargin).show(false)

    // create arbitrary numbers of columns dynamically
    val selectedColumns = simpleColors.map(color => col("Description").contains(color.toUpperCase).alias(s"is_$color")):+ expr("*")
    df.select(selectedColumns:_*)
      .where(col("is_white").or(col("is_red")))
      .show(false)


    /*
     * working with dates and timestamps
     */
    println("=== working with dates and timestamps ===")
    val dateDF = spark.range(10)
      .withColumn("today", current_date())
      .withColumn("now", current_timestamp())

    dateDF.createOrReplaceTempView("dateTable")
    dateDF.printSchema()

    // add and subtract date
    dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(3)
    // or
    spark.sql(
      """
        |select date_sub(today, 5), date_add(today, 5)
        |from dateTable
        |limit 3
      """.stripMargin).show()

    // difference between two dates
    dateDF.select(
      to_date(lit("2019-03-26")).alias("start"),  // will not be shown in console
      to_date(lit("2019-04-30")).alias("end")  // will not be shown in console
    ).select(datediff(col("end"), col("start")), months_between(col("end"), col("start")))
      .show(3)  // will only show datediff and months_between in console

    spark.sql(
      """
        |select to_date('2019-03-26') as start, to_date('2019-04-30') as end, datediff('2019-04-30', '2019-03-26'), months_between('2019-04-30', '2019-03-26')
        |from dateTable
        |limit 3
      """.stripMargin).show()

    // Java SimpleDateFormat
    val dateFormat = "yyyy-dd-MM"
    val cleanDateDF = spark.range(1).select(
      to_date(lit("2019-12-11"), dateFormat).alias("date"),
      to_date(lit("2019-20-12"), dateFormat).alias("date2")
    )
    cleanDateDF.show()
    cleanDateDF.createOrReplaceTempView("dateTable2")
    spark.sql(
      """
        |select to_date(date, 'yyyy-dd-MM'), to_date(date2, 'yyyy-dd-MM'), to_date(date), to_date(date2)
        |from dateTable2
      """.stripMargin).show()

    cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()
    // or
    spark.sql("select to_timestamp(date, 'yyyy-dd-MM') from dateTable2").show()

    // casting between dates and timestamps
    cleanDateDF.select(col("date").cast(TimestampType)).show()
    // or
    spark.sql("select cast(date as timestamp) from dateTable2").show()


    /*
     * working with nulls in data
     */
    println("=== working with nulls in data ===")
    // coalesce: select the first non-null value from a set of columns
    df.select(coalesce(col("Description"), col("CustomerId"))).show()

    spark.sql(
      """
        |select ifnull(null, 'return the second value if the first is null') as a,
        |       nullif('if two values are equal return null; otherwise return the second one', 'if two values are equal return null; otherwise return the second one') as b,
        |       nvl(null, 'return the second value if the first is null') as c,
        |       nvl2('return the second value if the first is not null; otherwise, return the last', 'second_value', 'last_value') as d
        |from dfTable
        |limit 1
      """.stripMargin).show(false)

    // drop a row if any of the values are null
    df.na.drop()
    // or
    df.na.drop("any")

    // drops the row only if all values are null or NaN for that row
    df.na.drop("all")

    // apply drop to certain sets of columns
    df.na.drop("all", Seq("StockCode", "InvoiceNo"))

    // fill
    df.na.fill("All Null values become this string")
    // apply fill to certain sets of columns
    df.na.fill(5, Seq("StockCode", "InvoiceNo"))
    // fill using map
    df.na.fill(Map("StockCode" -> 5, "Description" -> "No value"))

    // replace
    df.na.replace("Description", Map("" -> "UNKNOWN"))


    /*
     * working with complex types
     */
    println("=== working with complex types ===")
    // structs: DataFrames within DataFrames
    df.selectExpr("(Description, InvoiceNo) as complex", "*").show(3, false)
    // or
    df.selectExpr("struct(Description, InvoiceNo) as complex", "*").show(3, false)

    val complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))
    complexDF.show(3, false)
    complexDF.createOrReplaceTempView("complexTable")

    complexDF.select("complex.Description").show(3, false)
    // or
    complexDF.select(col("complex").getField("Description")).show(3, false)

    // select all fields separately
    complexDF.select("complex.*").show(3, false)
    // or
    spark.sql("select complex.* from complexTable limit 3").show(false)

    // arrays
    df.select(split(col("Description"), " ").alias("array_col"))
      .selectExpr("array_col[0]")
      .show(3, false)
    // or
    spark.sql(
      """
        |select split(Description, ' ')[0]
        |from dfTable
        |limit 3
      """.stripMargin).show(false)
    // array_contains
    df.select(array_contains(split(col("Description"), " "), "WHITE")).show(3, false)
    // or
    spark.sql(
      """
        |select array_contains(split(Description, ' '), 'WHITE')
        |from dfTable
        |limit 3
      """.stripMargin).show(false)
    // explode the array
    df.withColumn("splitted", split(col("Description"), " "))
      .withColumn("exploded", explode(col("splitted")))
      .select("Description", "splitted", "exploded")
      .show(false)

    // maps
    df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map")).show(3, false)
    // or
    spark.sql(
      """
        select map(Description, InvoiceNo) as complex_map
        from dfTable where Description is not null
        limit 3
      """).show(false)
    // query value through key of the map
    df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
      .selectExpr("complex_map['WHITE METAL LANTERN']")
      .show(3, false)
    // explode the map
    df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
      .selectExpr("explode(complex_map)")
      .show( false)


    /*
     * working with json
     */
    println("=== working with json ===")
    val jsonDF = spark.range(1).selectExpr("""'{"myJSONKey" : {"myJSONValue" : [1, 2, 3]}}' as jsonString""")
    jsonDF.select(
      get_json_object(col("jsonString"), "$.myJSONKey.myJSONValue[1]") as "column",
      json_tuple(col("jsonString"), "myJSONKey")
    ).show(3, false)

    // turn struct type into a json string and parse json string back to struct type
    val parseSchema = StructType(Array(
      StructField("InvoiceNo", StringType, true),
      StructField("Description", StringType, true)
    ))
    df.selectExpr("(InvoiceNo, Description) as myStruct")
      .select(to_json(col("myStruct")).alias("newJSON"))
      .select(from_json(col("newJSON"), parseSchema), col("newJSON"))
      .show(3, false)


    /*
     * working with UDFs
     */
    println("=== working with UDFs ===")
    val udfExampleDF = spark.range(5).toDF("num")  // "num" is the name of the column
    def power3(number: Double): Double = number * number * number
    // register the UDF as a DataFrame function through udf()
    val power3udf = udf(power3(_: Double): Double)
    udfExampleDF.select(power3udf(col("num"))).show(false)

    // register the UDF as a Spark SQL function through spark.udf.register()
    spark.udf.register("power3", power3(_: Double): Double)
    udfExampleDF.selectExpr("power3(num)").show(false)
    // or
    udfExampleDF.createOrReplaceTempView("udfExampleTable")
    spark.sql("select power3(num) from udfExampleTable").show(false)
  }
}
