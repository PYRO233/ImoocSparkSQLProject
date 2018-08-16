package com.imooc.spark_log_analysis

import com.imooc.spark_log_analysis.utils.DateUtils
import org.apache.spark.sql.SparkSession


/**
  * 第一步清洗：抽取出我们所需要的指定列的数据
  */
object SparkStatFormatJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkStatFormatJob")
      .master("local[2]")
      .getOrCreate()


    // RDD
    val acccess = spark.sparkContext.textFile("file:///Users/rocky/data/imooc/10000_access.log")

    acccess.map(line => {
      // 按字段切分
      val splits = line.split(" ")
      val ip = splits(0)
      /**
        * 原始日志的第三个和第四个字段拼接起来就是完整的访问时间：
        * [10/Nov/2016:00:01:02 +0800] ==> yyyy-MM-dd HH:mm:ss
        */
      val time = splits(3) + " " + splits(4)
      val url = splits(11).replaceAll("\"", "")
      val traffic = splits(9)

      DateUtils.parse(time) + "\t" + url + "\t" + traffic + "\t" + ip
    }).saveAsTextFile("file:///Users/rocky/data/imooc/output/")


    spark.stop()
  }
}
