package com.imooc.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}



// SQLContext，spark1.x
object SQLContextApp {
  def main(args: Array[String]): Unit = {


    // 设置参数：file:////usr/local/etc/spark-2.3.1-bin-hadoop2.7/examples/src/main/resources/people.json
    val path = args(0)

    //1)创建相应的Context
    val sparkConf = new SparkConf()

    //在测试或者生产中，AppName和Master是通过脚本进行指定
    sparkConf.setAppName("SQLContextApp").setMaster("local[2]")

    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)

    //2)相关的处理: json
    // 返回DataFrame
    val people = sqlContext.read.format("json").load(path)

    people.printSchema()
    people.show()


    //3)关闭资源
    sc.stop()
  }
}
