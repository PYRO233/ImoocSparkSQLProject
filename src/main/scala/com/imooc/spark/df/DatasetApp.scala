package com.imooc.spark.df

import org.apache.spark.sql.SparkSession

object DatasetApp {

  case class People(name: String,
                    age: Int,
                    job: String)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("DatasetApp")
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    //注意：需要导入隐式转换
    import spark.implicits._

    //spark如何解析csv文件？
    val peopleDF = spark.read.option("header", "true").option("inferSchema", "true").csv("file:////usr/local/etc/spark-2.3.1-bin-hadoop2.7/examples/src/main/resources/people.csv")

    peopleDF.show()


    val peopleDS = peopleDF.as[People]

    peopleDS.map(line => line.name).show

    spark.stop()
  }
}
