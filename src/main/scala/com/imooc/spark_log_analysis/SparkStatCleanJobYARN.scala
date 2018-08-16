package com.imooc.spark_log_analysis

import com.imooc.spark_log_analysis.utils.AccessConvertUtil
import org.apache.spark.sql.SparkSession

/**
  * 第二步：使用Spark完成我们的数据清洗操作，将数据从text转换为parquet数据
  */
object SparkStatCleanJob {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: SparkStatCleanJobYARN <inputPath> <outputPath>")
      System.exit(1)
    }
    val Array(inputPath, outputPath) = args
    val spark = SparkSession
      .builder().config("spark.sql.parquet.compression.codec", "gzip").getOrCreate()

    val accessRDD =
      spark.sparkContext.textFile(inputPath)

    // RDD => DF
    val accessDF = spark.createDataFrame(accessRDD.map(line => AccessConvertUtil.parseLog(line)), AccessConvertUtil.struct)

    // 保存成 parquet 格式，按 day 分区
    accessDF.write.format("parquet").partitionBy("day").save(outputPath)

    // 分成 1 个文件，保存成 parquet 格式，按 day 分区
//    accessDF.coalesce(1).write.format("parquet").partitionBy("day").save()

    // 分成 1 个文件，保存成 parquet 格式，覆盖已有文件，按 day 分区
//    accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite)partitionBy("day").save()

    spark.stop()
  }

}
