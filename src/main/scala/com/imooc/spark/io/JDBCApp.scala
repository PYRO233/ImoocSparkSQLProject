package com.imooc.spark.io

import org.apache.spark.sql.SparkSession

object JDBCApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("JDBCApp").master("local[2]").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    val jdbdDF = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/hive").option("dbtable","database.table").option("user", "username").option("password", "password").option("driver", "com.mysql.jdbc.Driver").load()
  }
}
