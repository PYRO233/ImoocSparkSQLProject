package com.imooc.spark_log_analysis

import com.imooc.spark_log_analysis.utils.AccessConvertUtil
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
  * 第二步：使用Spark完成我们的数据清洗操作，将数据从text转换为parquet数据
  */
object SparkStatCleanJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkStatCleanJob")
      .master("local[2]")
      .getOrCreate()

    val accessRDD =
      spark.sparkContext.textFile("/Users/rocky/data/imooc/access.log")

    // RDD => DF
    val accessDF = spark.createDataFrame(accessRDD.map(line => AccessConvertUtil.parseLog(line)), AccessConvertUtil.struct)

    // 保存成 parquet 格式，按 day 分区
    accessDF.write.format("parquet").partitionBy("day").save()

    // 分成 1 个文件，保存成 parquet 格式，按 day 分区
//    accessDF.coalesce(1).write.format("parquet").partitionBy("day").save()

    // 分成 1 个文件，保存成 parquet 格式，覆盖已有文件，按 day 分区
//    accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite)partitionBy("day").save()

    spark.stop()
  }

}
