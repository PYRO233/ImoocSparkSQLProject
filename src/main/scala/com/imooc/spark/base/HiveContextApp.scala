package com.imooc.spark.base

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * HiveContext的使用，spark1.x
  * 使用时通过 --jars 把mysql的驱动传递到classpath
  */
object HiveContextApp {
  def main(args: Array[String]): Unit = {

    // 1. 创建相应的Context
    val sparkConf = new SparkConf()

    sparkConf.setAppName("HiveContextApp").setMaster("local[2]")

    val sc = new SparkContext(sparkConf)

    val hiveContext = new HiveContext(sc)

    //2)相关的处理:
    hiveContext.table("emp").show()

    //3)关闭资源
    sc.stop()
  }
}
