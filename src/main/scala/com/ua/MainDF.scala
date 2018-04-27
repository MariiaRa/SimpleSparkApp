package com.ua

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object MainDF {

  /**
    *
    * @param ss   sparkSession
    * @param file csv file with input data
    * @return CropsData typed Dataset
    */

  private def createDS(ss: SparkSession, file: String) = {
    import ss.implicits._

    val regions: List[String] = List("World", "Central America", "Central Asia", "Americas", "Eastern Africa", "Eastern Asia",
      "Eastern Europe", "European Union", "Europe", "Australia and New Zealand", "Middle Africa", "Net Food Importing Developing Countries",
      "Small Island Developing States", "Least Developed Countries", "countries", "Low Income Food Deficit Countries",
      "Northern Africa", "Northern America", "Northern Europe", "South Africa", "South America", "South-Eastern Asia", "Southern Africa",
      "Southern Asia", "Southern Europe", "Western Africa", "Western Asia", "Western Europe", "Western Sahara")

    //user defined schema
    val customSchema = new StructType()
      .add("country_or_area", StringType, true)
      .add("element_code", IntegerType, true)
      .add("element", StringType, true)
      .add("year", IntegerType, true)
      .add("unit", StringType, true)
      .add("value", LongType, true)
      .add("value_footnotes", StringType, true)
      .add("category", StringType, true)

    //create DataFrames
    val DFCsv = ss.read.format("csv")
      .option("sep", ",")
      .option("header", "true")
      .schema(customSchema)
      .load(file)

    //select columns of interes
    val DFCsvSelect = DFCsv.select($"country_or_area".alias("Country"), $"year".alias("Year"), $"value".alias("Value"), $"category".alias("Category"))

    //trim
    val DFCsvTrimmed = DFCsvSelect.withColumn("Country", trim(DFCsvSelect("Country")))

    //filter out regions
    val countries = DFCsvTrimmed.filter(row => !regions.contains(row.getAs[String]("Country")))
    countries
  }

  /**
    *
    * @param src path directory with files to merge
    * @param dst path to file destination
    */

  def merge(src: String, dst: String): Unit = {
    val srcPath: Path = new Path(src)
    val dstPath: Path = new Path(dst)
    val hadoopConfig = new Configuration()
    val fs = FileSystem.get(new Configuration())
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, srcPath, hdfs, dstPath, true, hadoopConfig, "\n")
  }


  def main(args: Array[String]): Unit = {

    val category = args(0)
    val year = args(1)
    val country = args(2)
    val file = args(3)

    val sparkSession = SparkSession.builder
        .master("local")
      .appName("simpleSparkApp")
      .getOrCreate()

    import sparkSession.implicits._

    val countries = createDS(sparkSession, file)

    //total production
    val totalProduction = countries.where($"category" === category && $"year" === year).agg(sum("value").cast("long").alias("Total_Production"))

    //min, max, avg
    val minMaxAvg = countries.where($"category" === category && $"Country" === country).agg(min("value").cast("long").alias("Min_Production"), max("value").cast("long").alias("Max_Production"), avg("value").cast("long").alias("Average_Production"))

    //top productive year in world
    val topProducers = countries.where($"category" === category && $"year" === year).sort(desc("value"))

    //top productive year by country
    val topYear = countries.where($"category" === category && $"Country" === country).sort(desc("value"))

    val srcDir = "/storage/report"
    val dstFile = "/storage/sparkAppReport2.txt"

    minMaxAvg.write
      .format("csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .save(srcDir)

    totalProduction.write
      .format("csv")
      .mode("append")
      .option("header", "true")
      .option("delimiter", "\t")
      .save(srcDir)

    topYear
      .limit(1)
      .write.format("csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .mode("append")
      .save(srcDir)

    topProducers
      .limit(10)
      .write.format("csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .mode("append")
      .save(srcDir)

    merge(srcDir, dstFile)

    sparkSession.stop()
  }
}
