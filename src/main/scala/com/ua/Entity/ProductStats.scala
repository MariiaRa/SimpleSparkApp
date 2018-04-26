package com.ua.Entity

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}

class ProductStats{

  /**
    *
    * @param input    input Dataset
    * @param category name of category to search in Dataset
    * @param year     year of interest to search in Dataset
    * @return DataFrame with column "Total_Production"
    */

  def getTotalProduction(input: Dataset[ProductData], category: String, year: Int): DataFrame = {
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

  def getMinMaxAvd(input: Dataset[ProductData], category: String, country: String): DataFrame = {
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

  def getTopProducers(input: Dataset[ProductData], category: String, year: Int): Dataset[ProductData] = { //top productive year in world
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

  def getTopYear(input: Dataset[ProductData], category: String, country: String): Dataset[ProductData] = {
    input.filter(crop => crop.element == "Production Quantity"
      && crop.category.equals(category)
      && crop.country_or_area == country).sort(desc("value"))
  }
}
