package com.imooc.spark.base

import org.apache.spark.sql.SparkSession

/**
  * SparkSession的使用
  *
  * SparkSession是spark2.0以后默认的的统一客户端程序入口。
  *
  * sparkSession是HiveContext和sqlContext的统一入口
  * sparkContext可以通过spark.sparkContext获得
  */
object SparkSessionApp {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("SparkSessionApp")
      .master("local[2]")
      .getOrCreate()

    val people = spark.read.json("file:////usr/local/etc/spark-2.3.1-bin-hadoop2.7/examples/src/main/resources/people.json")
    people.show()

    spark.stop()
  }
}
