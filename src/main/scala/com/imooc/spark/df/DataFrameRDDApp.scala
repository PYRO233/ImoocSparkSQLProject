package com.imooc.spark.df

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * DataFrame和RDD的互操作
  */
object DataFrameRDDApp {
  case class Info(id: Int, name: String, age: Int)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DataFrameRDDApp").master("local[2]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")


    // RDD ==> DataFrame
    val rdd = spark.sparkContext.textFile("file:////usr/local/etc/spark-2.3.1-bin-hadoop2.7/examples/src/main/resources/infos.txt")

    // 方法1
//    inferReflection(spark, rdd)

    // 方法2
    val infoDF: DataFrame = program(spark, rdd)

    infoDF.printSchema()
    infoDF.show()
    infoDF.filter(infoDF.col("age") > 30).show()

    // 纯用sql
    infoDF.createOrReplaceTempView("infos")
    spark.sql("select * from infos where age > 30").show()

    spark.stop()
  }

  private def program(spark: SparkSession, rdd: RDD[String]) = {
    // 方法2
    val infoRDD = rdd.map(_.split(",")).map(line => Row(line(0).toInt, line(1), line(2).toInt))
    val structType = StructType(Array(StructField("id", IntegerType, true), StructField("name", StringType, true), StructField("age", IntegerType, true)))
    val infoDF = spark.createDataFrame(infoRDD, structType)
    infoDF
  }

  private def inferReflection(spark: SparkSession, rdd: RDD[String]) = {
    //注意：需要导入隐式转换
    import spark.implicits._

    val infoDF = rdd.map(_.split(",")).map(line => Info(line(0).toInt, line(1), line(2).toInt)).toDF()

  }
}
