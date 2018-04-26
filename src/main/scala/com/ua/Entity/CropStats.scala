package com.ua.Entity

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class CropStats {

  /**
    *
    * @param ss   sparkSession
    * @param file csv file with input data
    * @return CropsData typed Dataset
    */

  def createDS(ss: SparkSession, file: String): Dataset[CropsData] = {
    import ss.implicits._

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
    val DFCsv = ss.read.format("csv")
      .option("sep", ",")
      .option("header", "true")
      .schema(customSchema)
      .load(file)

    //replace nulls
    val noNullDFCsv = DFCsv.na.fill("blank", Seq("country_or_area"))
      .na.fill(0, Seq("element_code"))
      .na.fill(0, Seq("year"))
      .na.fill(0, Seq("value"))
      .na.fill("blank", Seq("value_footnotes"))

    // val dataSet = DFCsv.as[CropsData]

    //create Dataset
    val DS = noNullDFCsv.map(row => CropsData(row.getAs[String](0).trim, row.getAs[Integer](1), row.getAs[String](2),
      row.getAs[Integer](3), row.getAs[String](4), row.getAs[Long](5), row.getAs[String](6),
      row.getAs[String](7)))

    //filter out regions
    val countries = DS.filter(crop => !crop.country_or_area.equals("World")
      && !crop.country_or_area.contains("Asia") && !crop.country_or_area.contains("Africa")
      && !crop.country_or_area.equals("Asia") && !crop.country_or_area.contains("America")
      && !crop.country_or_area.contains("Americas") && !crop.country_or_area.contains("Europe")
      && !crop.country_or_area.equals("European Union") && !crop.country_or_area.equals("Australia and New Zealand")
      && !crop.country_or_area.equals("Small Island Developing States") && !crop.country_or_area.contains("Countries")
      && !crop.country_or_area.contains("countries") && !crop.country_or_area.equals("Low Income Food Deficit Countries")
    )
    countries
  }

  /**
    *
    * @param input    input Dataset
    * @param category name of category to search in Dataset
    * @param year     year of interest to search in Dataset
    * @return DataFrame with column "Total_Production"
    */

  def getTotalProduction(input: Dataset[CropsData], category: String, year: Int): DataFrame = {
    input.filter(crop => crop.element == "Production Quantity" && crop.category.equals(category) &&
      crop.year == year).agg(sum("value").alias("Total_Production"))
  }

  /**
    *
    * @param input    input Dataset
    * @param category name of category to search in Dataset
    * @param country  name of country to search in Dataset
    * @return DataFrame with statistics (min, max, avg)
    */

  def getMinMaxAvd(input: Dataset[CropsData], category: String, country: String): DataFrame = {
    input.filter(crop => crop.element == "Production Quantity"
      && crop.category.equals(category)
      && crop.country_or_area == country)
      .agg(min("value").alias("Min_Production"), max("value").alias("Max_Production"), avg("value").alias("Average_Production"))
  }

  /**
    *
    * @param input    input Dataset
    * @param category name of category to search in Dataset
    * @param year     year of interest to search in Dataset
    * @return Dataset with sorted value of production to find top producers by category
    */

  def getTopProducers(input: Dataset[CropsData], category: String, year: Int): Dataset[CropsData] = { //top productive year in world
    input.filter(crop => crop.element == "Production Quantity"
      && crop.category.equals(category)
      && crop.year == year).sort(desc("value"))
  }

  /**
    *
    * @param input    input Dataset
    * @param category name of category to search in Dataset
    * @param country  name of country to search in Dataset
    * @return Dataset with sorted value of production to find top productive year by country and category
    */

  def getTopYear(input: Dataset[CropsData], category: String, country: String): Dataset[CropsData] = {
    input.filter(crop => crop.element == "Production Quantity"
      && crop.category.equals(category)
      && crop.country_or_area == country).sort(desc("value"))
  }
}
