package com.ua

import com.ua.Entity.CropStats
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val category = args(0)
    val year = args(1).toInt
    val country = args(2)
    val file = args(3)

    val sparkSession = SparkSession.builder.
      master("yarn")
      .appName("simpleSparkApp")
      .getOrCreate()

    val statistics = new CropStats

    val input = statistics.createDS(sparkSession, file)

    val minMaxAvg = statistics.getMinMaxAvd(input, category, country)
    minMaxAvg.write
      .format("csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .save("report")

    val topProducers = statistics.getTopProducers(input, category, year)
    topProducers
      .limit(10)
      .write.format("csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .mode("append")
      .save("report")

    val topYear = statistics.getTopYear(input, category, country)
    topYear
      .limit(1)
      .write.format("csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .mode("append")
      .save("report")

    val totalProduction = statistics.getTotalProduction(input, category, year)
    totalProduction.write
      .format("csv")
      .mode("append")
      .option("header", "true")
      .option("delimiter", "\t")
      .save("report")

    val src = "hdfs://alpha.gemelen.net:8020/user/hdfs/report"
    val dst = "hdfs://alpha.gemelen.net:8020/user/hdfs/sparkAppReport1.txt"

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
