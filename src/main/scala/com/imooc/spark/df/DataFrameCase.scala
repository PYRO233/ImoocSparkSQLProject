package com.imooc.spark.df

import org.apache.spark.sql.SparkSession

/**
  * RDD to DataFrame以及DataFrame操作
  */
object DataFrameCase {
  case class Student(id: Int, name: String, phone: String, email: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("DataFrameCase")
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    // RDD ==> DataFrame
    val rdd =
      spark.sparkContext.textFile("file:////usr/local/etc/spark-2.3.1-bin-hadoop2.7/examples/src/main/resources/student.data")

    //注意：需要导入隐式转换
    import spark.implicits._
    val studentDF = rdd
      .map(_.split("\\|"))
      .map(line => Student(line(0).toInt, line(1), line(2), line(3)))
      .toDF()

    //show默认只显示前20条
    studentDF.show
    studentDF.show(30)
    studentDF.show(30, false)

    // 未生效
    studentDF.take(3)
    studentDF.first()
    studentDF.head(3)

    studentDF.select("email").show(30, false)

    // name为 空 或 NULL
    studentDF.filter("name=''").show
    studentDF.filter("name='' OR name='NULL'").show

    //name以M开头的人
    studentDF.filter("SUBSTR(name,0,1)='B'").show

    // 按 name 排序
    studentDF.sort(studentDF("name")).show
    studentDF.sort(studentDF("name").desc).show

    // 多字段排序
    studentDF.sort("name", "id").show
    studentDF.sort(studentDF("name").asc, studentDF("id").desc).show

    studentDF.select(studentDF("name").as("student_name")).show

    val studentDF2 = rdd
      .map(_.split("\\|"))
      .map(line => Student(line(0).toInt, line(1), line(2), line(3)))
      .toDF()

    // 默认 innerJoin
    studentDF
      .join(studentDF2, studentDF.col("id") === studentDF2.col("id"))
      .show

    spark.stop()

  }

}
