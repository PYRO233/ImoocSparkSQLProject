package com.imooc.spark.io

import org.apache.spark.sql.SparkSession

/**
  * Schema Infer
  * "spark.sql.sources.partitionColumnTypeInference.enabled" 默认是 true
  * json_schema_infer.json不是一个规范的表，有缺值
  */
object SchemaInferApp {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("SchemaInferApp")
      .master("local[2]")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", false)
      .getOrCreate()

    val df = spark.read
      .format("json")
      .load("file:///Users/rocky/data/json_schema_infer.json")

    df.printSchema()

    df.show()

    spark.stop()
  }
}
