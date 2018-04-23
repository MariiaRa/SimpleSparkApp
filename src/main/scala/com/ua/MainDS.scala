package com.ua

import com.ua.Entity.CropsData
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object MainDS {
  def main(args: Array[String]): Unit = {

    val category = args(0)
    val year = args(1)
    val country = args(2)
    val file = args(3)

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("spark session example")
      .getOrCreate()

    import sparkSession.implicits._

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
    val DFCsv = sparkSession.read.format("csv")
      .option("sep", ",")
      .option("header", "true")
      .schema(customSchema)
      .load(file)

    val noNullDSCsv = DFCsv.na.fill("blank", Seq("country_or_area"))
      .na.fill(0, Seq("element_code"))
      .na.fill(0, Seq("year"))
      .na.fill(0, Seq("value"))
      .na.fill("blank", Seq("value_footnotes"))

    // val dataSet = DFCsv.as[CropsData]

    val DS2 = noNullDSCsv.map(row => CropsData(row.getAs[String](0).trim, row.getAs[Integer](1), row.getAs[String](2),
      row.getAs[Integer](3), row.getAs[String](4), row.getAs[Long](5), row.getAs[String](6), row.getAs[String](7)))
    //  DS2.show()

    //select columns of interest
    val selected = DS2.select($"country_or_area".alias("Country"), $"year".alias("Year"), $"element".alias("Element"),
      $"value".alias("Value"), $"category".alias("Category"))

    //filter out regions
    val countries = selected.where(
      !$"Country".contains("World") &&
        !selected.col("Country").contains("Asia") &&
        !selected.col("Country").contains("Africa") &&
        !selected.col("Country").contains("America") &&
        !selected.col("Country").contains("Americas") &&
        !selected.col("Country").contains("Europe") &&
        !selected.col("Country").contains("European Union") &&
        !selected.col("Country").contains("Australia and New Zealand") &&
        !selected.col("Country").like("%Countries") &&
        !selected.col("Country").like("%countries") &&
        !selected.col("Country").contains("Small Island Developing States")
    )

    //total production
    val totalProduction = countries.where($"Category" === category && $"Year" === year && $"Element" === "Production Quantity")
      .agg(sum("Value").alias("Total_Production"))

    //  min, max, avg
    val minMaxAvg = selected.where($"category" === category && $"Country" === country && $"Element" === "Production Quantity")
      .agg(min("value").alias("Min Production"), max("value").alias("Max Production"), avg("value").alias("Average Production"))


    //top productive year in world
    val topProducers = countries.where($"category" === category && $"year" === year && $"Element" === "Production Quantity").sort(desc("value"))

    //top productive year by country
    val topYear = countries.where($"category" === category && $"Country" === country && $"Element" === "Production Quantity").sort(desc("value"))

    minMaxAvg.write
      .format("csv")
      .option("header", "true")
      .save("report2")

    totalProduction.write
      .format("csv")
      .mode("append")
      .option("header", "true")
      .save("report2")

    topYear
      .limit(1)
      .write.format("csv")
      .option("header", "true")
      .mode("append")
      .save("report2")

    topProducers
      .limit(10)
      .write.format("csv")
      .option("header", "true")
      .mode("append")
      .save("report2")
  }
}
