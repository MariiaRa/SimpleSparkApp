package com.ua

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object MainDF {
  def main(args: Array[String]): Unit = {

    //get args
    val category = args(0)
    val year = args(1)
    val country = args(2)
    val file = args(3)

    //entry point into all functionality in Spark is the SparkSession class
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("spark session example")
      .getOrCreate()

    //user defined schema
    val customSchema = StructType(Array(
      StructField("country_or_area", StringType, true),
      StructField("element_code", IntegerType, true),
      StructField("element", StringType, true),
      StructField("year", IntegerType, true),
      StructField("unit", StringType, true),
      StructField("value", LongType, true),
      StructField("value_footnotes", StringType, true),
      StructField("category", StringType, true)))

    //create DataFrames
    val animalDFCsv = sparkSession.read.format("csv")
      .option("sep", ",")
      .option("header", "true")
      .schema(customSchema)
      .load(file)

    import sparkSession.implicits._

    //select columns of interes
    val selected = animalDFCsv.select($"country_or_area".alias("Country"), $"year".alias("Year"), $"value".alias("Value"), $"category".alias("Category"))

    //trim
    val animalDFCsvTrimmed = selected.withColumn("Country", trim(selected("Country")))

    //filter out regions
    val countries = animalDFCsvTrimmed.where(
      !$"Country".contains("World") &&
        !animalDFCsvTrimmed.col("Country").contains("Asia") &&
        !animalDFCsvTrimmed.col("Country").contains("Africa") &&
        !animalDFCsvTrimmed.col("Country").contains("America") &&
        !animalDFCsvTrimmed.col("Country").contains("Americas") &&
        !animalDFCsvTrimmed.col("Country").contains("Europe") &&
        !animalDFCsvTrimmed.col("Country").contains("European Union") &&
        !animalDFCsvTrimmed.col("Country").contains("Australia and New Zealand") &&
        !animalDFCsvTrimmed.col("Country").like("%Countries") &&
        !animalDFCsvTrimmed.col("Country").like("%countries") &&
        !animalDFCsvTrimmed.col("Country").contains("Small Island Developing States")
    )

    //total production
    val totalProduction = countries.where($"category" === category && $"year" === year).agg(sum("value").cast("long").alias("Total_Production"))

    //  min, max, avg
    val minMaxAvg = selected.where($"category" === category && $"Country" === country).agg(min("value").cast("long").alias("Min Production"), max("value").cast("long").alias("Max Production"), avg("value").cast("long").alias("Average Production"))
    //  selected.where(($"category" === "chickens" && $"Country" === "Egypt")).agg(min("value"), max("value"), avg("value")).show()

    //top productive year in world
    val topProducers = countries.where($"category" === category && $"year" === year).sort(desc("value"))

    //top productive year by country
    val topYear = countries.where($"category" === category && $"Country" === country).sort(desc("value"))

    minMaxAvg.write
      .format("csv")
      .option("header", "true")
      .save("report1")

    totalProduction.write
      .format("csv")
      .mode("append")
      .option("header", "true")
      .save("report1")

    topYear
      .limit(1)
      .write.format("csv")
      .option("header", "true")
      .mode("append")
      .save("report1")

    topProducers
      .limit(10)
      .write.format("csv")
      .option("header", "true")
      .mode("append")
      .save("report1")
  }
}
