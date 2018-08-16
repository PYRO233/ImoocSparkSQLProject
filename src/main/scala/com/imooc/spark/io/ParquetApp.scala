package com.imooc.spark.io

import org.apache.spark.sql.SparkSession

object ParquetApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ParquetApp").master("local[2]").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    val userDF = spark.read.format("parquet").load("file:///Users/td/IdeaProjects/ImoocSparkSQLProject/src/main/resources/users.parquet")

    userDF.printSchema

    userDF.show

    userDF.select("name", "favorite_color").show

    //======================== 保存成json ========================
//    userDF.select("name", "favorite_color").write.format("json").save()
//    userDF.write.format("json").save()
  }
}
