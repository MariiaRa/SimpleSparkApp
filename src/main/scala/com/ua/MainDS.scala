package com.ua

import com.ua.Entity.CropsData
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object MainDS {
  def main(args: Array[String]): Unit = {

    val category = args(0)
    val year = args(1).toInt
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
                              row.getAs[Integer](3), row.getAs[String](4), row.getAs[Long](5), row.getAs[String](6),
                              row.getAs[String](7)))

    //filter out regions
    val countries = DS2.filter(crop => !crop.country_or_area.equals("World")
      && !crop.country_or_area.contains("Asia") && !crop.country_or_area.contains("Africa")
      && !crop.country_or_area.equals("Asia") && !crop.country_or_area.contains("America")
      && !crop.country_or_area.contains("Americas") && !crop.country_or_area.contains("Europe")
      && !crop.country_or_area.equals("European Union") && !crop.country_or_area.equals("Australia and New Zealand")
      && !crop.country_or_area.equals("Small Island Developing States") && !crop.country_or_area.contains("Countries")
      && !crop.country_or_area.contains("countries") && !crop.country_or_area.equals("Low Income Food Deficit Countries")
    )

    //total production
    val totalProduction = countries.filter(crop => crop.element == "Production Quantity" && crop.category.equals(category) &&
      crop.year == year).agg(sum("value").alias("Total_Production"))

    //  min, max, avg
    val minMaxAvg = countries.filter(crop => crop.element == "Production Quantity"
      && crop.category.equals(category)
      && crop.country_or_area == country)
      .agg(min("value").alias("Min Production"), max("value").alias("Max Production"), avg("value").alias("Average Production"))

    //top productive year in world
    val topProducers = countries.filter(crop => crop.element == "Production Quantity"
      && crop.category.equals(category)
      && crop.year == year).sort(desc("value"))

    //top productive year by country
    val topYear = countries.filter(crop => crop.element == "Production Quantity"
      && crop.category.equals(category)
      && crop.country_or_area == country).sort(desc("value"))

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
