package com.ua

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
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

    val sparkSession = SparkSession.builder.
      master("yarn")
      .appName("simpleSparkApp")
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
    val DFCsv = sparkSession.read.format("csv")
      .option("sep", ",")
      .option("header", "true")
      .schema(customSchema)
      .load(file)

    import sparkSession.implicits._

    //select columns of interes
    val DFCsvSelect = DFCsv.select($"country_or_area".alias("Country"), $"year".alias("Year"), $"value".alias("Value"), $"category".alias("Category"))

    //trim
    val DFCsvTrimmed = DFCsvSelect.withColumn("Country", trim(DFCsvSelect("Country")))

    //filter out regions
    val countries = DFCsvTrimmed.where(
      !$"Country".contains("World") &&
        !DFCsvTrimmed.col("Country").contains("Asia") &&
        !DFCsvTrimmed.col("Country").contains("Africa") &&
        !DFCsvTrimmed.col("Country").contains("America") &&
        !DFCsvTrimmed.col("Country").contains("Americas") &&
        !DFCsvTrimmed.col("Country").contains("Europe") &&
        !DFCsvTrimmed.col("Country").contains("European Union") &&
        !DFCsvTrimmed.col("Country").contains("Australia and New Zealand") &&
        !DFCsvTrimmed.col("Country").like("%Countries") &&
        !DFCsvTrimmed.col("Country").like("%countries") &&
        !DFCsvTrimmed.col("Country").contains("Small Island Developing States")
    )

    //total production
    val totalProduction = countries.where($"category" === category && $"year" === year).agg(sum("value").cast("long").alias("Total_Production"))

    //min, max, avg
    val minMaxAvg = DFCsvSelect.where($"category" === category && $"Country" === country).agg(min("value").cast("long").alias("Min_Production"), max("value").cast("long").alias("Max_Production"), avg("value").cast("long").alias("Average_Production"))


    //top productive year in world
    val topProducers = countries.where($"category" === category && $"year" === year).sort(desc("value"))

    //top productive year by country
    val topYear = countries.where($"category" === category && $"Country" === country).sort(desc("value"))

    minMaxAvg.write
      .format("csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .save("report")

    totalProduction.write
      .format("csv")
      .mode("append")
      .option("header", "true")
      .option("delimiter", "\t")
      .save("report")

    topYear
      .limit(1)
      .write.format("csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .mode("append")
      .save("report")

    topProducers
      .limit(10)
      .write.format("csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .mode("append")
      .save("report")

    val src = "hdfs://alpha.gemelen.net:8020/user/hdfs/report"
    val dst = "hdfs://alpha.gemelen.net:8020/user/hdfs/sparkAppReport2.txt"

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

    merge(src, dst)

    sparkSession.stop()
  }
}
