# Spark Coding Tutorial

This tutorial shows you how to code with Spark (Scala).

If you would like to see tutorial about Spark concepts, go to [Spark Note](https://github.com/lizhanmit/learning-notes/blob/master/spark-note/spark-note.md).

Environment:

- Spark 2.4.0
- Scala: 2.12.3
- IDE: Scala IDE Eclipse

Code Review Sequence:

1. rddBasics package
2. dataFrameBasics package
3. datasetBasics package
4. onClusterBasics package
5. udfBasics package

---

## Installation

Modify log4j

1. Under `spark/conf` folder, copy log4j.properties.template file to log4j.properties.
2. Open log4j.properties, modify `log4j.rootCategory=INFO, console` to `log4j.rootCategory=ERROR, console`.

---

## Code in Spark Shell

```scala
// in spark shell

val intArray = Array(1,2,3)
val intRDD = sc.parallelize(intArray)
intRDD.first
intRDD.take(2)
intRDD.collect
intRDD.collect.foreach(println)
intRDD.partitions.size  // get number of partitions of intRDD


val intList = List(1,2,3,4)
val intListRDD = sc.parallelize(intList, 10)  // specify number of partitions as 10 instead of the default


val fileRDD = sc.textFile("/Users/zhanli/spark/README.md")
fileRDD.first
fileRDD.take(3)
fileRDD.take(3).foreach(println)


val data = Array(
     | "This is line one",
     | "This is line two",
     | "This is line three",
     | "This is the last line")
val dataRDD = sc.parallelize(data)
val filterRDD = dataRDD.filter(line => line.length > 15)
filterRDD.collect.foreach(println)
val mapRDD = dataRDD.map(line => line.split(" "))
mapRDD.collect  // return type: Array[Array[String]]
val flatMapRDD = dataRDD.flatMap(line => line.split(" "))
flatMapRDD.collect  // return type: Array[String]


val numArray = Array(1,1,2,2,3,3,4)
val numRDD = sc.parallelize(numArray)
val distinctElementsRDD = numRDD.distinct
distinctElementsRDD.collect
```

---

## Set Up Spark Projects in IDE

1. Download Scala IDE for Eclipse.
2. Create a Maven project.
3. Rename folder "java" to "scala" for both "main" and "test".
4. Right click the project -> "Configure" -> "Add Scala Nature" to add Scala library.
5. Add dependencies in pom.xml.

---

## Scala Object VS Scala App

When coding in IDE, you will be choosing to create a Scala Object or a Scala App under a package.

- Scala Object
  - With main function.
  - You are able to specify input parameters when executing the program.

- Scala App
  - Extends `App`.
  - Without main function.

---

## Read CSV File as Dataset

When reading a CSV file as a Dataset with a case class, make sure headers of the CSV file matches fields of the case class. If they are not same, use code to modify the header after converting the CSV file to DataFrame, e.g. removing spaces and lowering case the first character, and then convert to Dataset.   

---

## Spark UDF

Steps:

1. Define a function.
2. Convert the function into a UDF.
3. Apply the UDF to DataFrame or Dataset.

---

## Coalesce VS Repartition

### Coalesce

- Coalesce is fast in certain situations because it minimizes data movement.
- Coalesce changes the number of nodes by moving data from some partitions to existing partitions. This algorithm obviously **cannot increase** the number of partitions.
  - For example, `numbersDf.rdd.partitions.size` is 4. Even if you use `numbersDf.coalesce(6)`, it is still 4 rather than 6.
- Avoids a full data shuffle.

### Repartition

- Repartition can be used to either increase or decrease the number of partitions in a DataFrame.
- Does a full data shuffle.
- If there is only a little amount of data but the number of partitions is big, there will be many empty partitions.

### Real World Example

1. Suppose you have a data lake that contains 2 billion rows of data (1TB) split in 13,000 partitions.
2. You want to create a data puddle containing 2,000 rows of data for the purpose of development by random sampling of one millionth of the data lake.
3. Write the data puddle out to S3 for easy access.

```scala
val dataPuddle = dataLake.sample(true, 0.000001)
dataPuddle.write.parquet("s3a://my_bucket/puddle/")
```

4. Spark does not adjust the number of partitions, so the dataPuddle will also have 13,000 partitions, which means a lot of the partitions will be empty.
5. Not efficient to read or write thousands of empty text files to S3. Improve by repartitioning.

```scala
val dataPuddle = dataLake.sample(true, 0.000001)
val goodPuddle = dataPuddle.repartition(4)
goodPuddle.write.parquet("s3a://my_bucket/puddle/")
```

6. **Why choosing 4 partitions here?** 13,000 partitions / 1,000,000 = 1 partition. Then `number_of_partitions = number_of_cpus_in_cluster * 4` (2, 3 or 4).
7. **Why using repartition instead of coalesce here?** The data puddle is small. The repartition method returns equal sized text files, which are more efficient for downstream consumers.

**When filtering large DataFrames into smaller ones, you should almost always repartition the data.**

---

## Troubleshooting

- **Problem 1**

When running "hello world" example once after setting up the project, got such an error.

```
Exception in thread "main" java.lang.ArrayIndexOutOfBoundsException: 10582
...
```

Solution: Add such a dependency in pom.xml.

```
<dependency>
	<groupId>com.thoughtworks.paranamer</groupId>
	<artifactId>paranamer</artifactId>
	<version>2.8</version>
</dependency>
```
