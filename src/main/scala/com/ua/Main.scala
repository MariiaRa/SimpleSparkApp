package com.ua

import com.ua.Entity.{ProductStats, ProductData}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.{Dataset, SparkSession}

object Main {

  /**
    *
    * @param ss   sparkSession
    * @param file csv file with input data
    * @return CropsData typed Dataset
    */

  private def createDS(ss: SparkSession, file: String): Dataset[ProductData] = {
    import ss.implicits._

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

    //replace nulls
    val noNullDFCsv = DFCsv.na.fill("blank", Seq("country_or_area"))
      .na.fill(0, Seq("element_code"))
      .na.fill(0, Seq("year"))
      .na.fill(0, Seq("value"))
      .na.fill("blank", Seq("value_footnotes"))

    //create Dataset
    val DS = noNullDFCsv.map(row => ProductData(row.getAs[String](0).trim, row.getAs[Integer](1), row.getAs[String](2),
      row.getAs[Integer](3), row.getAs[String](4), row.getAs[Long](5), row.getAs[String](6),
      row.getAs[String](7)))

    //filter out regions
    val regions: List[String] = List("World", "Central America", "Central Asia", "Americas", "Eastern Africa", "Eastern Asia",
      "Eastern Europe", "European Union", "Europe", "Australia and New Zealand", "Middle Africa", "Net Food Importing Developing Countries",
      "Small Island Developing States", "Least Developed Countries", "countries", "Low Income Food Deficit Countries",
      "Northern Africa", "Northern America", "Northern Europe", "South Africa", "South America", "South-Eastern Asia", "Southern Africa",
      "Southern Asia", "Southern Europe", "Western Africa", "Western Asia", "Western Europe", "Western Sahara")

    DS.filter(x => !regions.contains(x.country_or_area))
  }

  /**
    *
    * @param src path directory with files to merge
    * @param dst path to file destination
    */

  private def merge(src: String, dst: String): Unit = {
    val srcPath: Path = new Path(src)
    val dstPath: Path = new Path(dst)
    val hadoopConfig = new Configuration()
    val fs = FileSystem.get(new Configuration())
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, srcPath, hdfs, dstPath, true, hadoopConfig, "\n")
  }

  def main(args: Array[String]): Unit = {
    val category = args(0)
    val year = args(1).toInt
    val country = args(2)
    val file = args(3)

    val sparkSession = SparkSession.builder
      .appName("simpleSparkApp")
      .getOrCreate()

    val statistics = new ProductStats

    val input = createDS(sparkSession, file)
    val srcDir = "/storage/report"
    val dstFile = "/storage/sparkAppReport1.txt"

    val minMaxAvg = statistics.getMinMaxAvd(input, category, country)
    minMaxAvg.write
      .format("csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .save(srcDir)

    val topProducers = statistics.getTopProducers(input, category, year)
    topProducers
      .limit(10)
      .write.format("csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .mode("append")
      .save(srcDir)

    val topYear = statistics.getTopYear(input, category, country)
    topYear
      .limit(1)
      .write.format("csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .mode("append")
      .save(srcDir)

    val totalProduction = statistics.getTotalProduction(input, category, year)
    totalProduction.write
      .format("csv")
      .mode("append")
      .option("header", "true")
      .option("delimiter", "\t")
      .save(srcDir)

    merge(srcDir, dstFile)

    sparkSession.stop()
  }
}
