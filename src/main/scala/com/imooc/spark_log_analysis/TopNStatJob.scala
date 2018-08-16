package com.imooc.spark_log_analysis

import com.imooc.spark_log_analysis.dao.StatDAO
import com.imooc.spark_log_analysis.domain.{DayCityVideoAccessStat, DayVideoAccessStat, DayVideoTrafficsStat}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

object TopNStatJob {
  def main(args: Array[String]): Unit = {

    if(args.length !=2) {
      println("Usage: TopNStatJobYARN <inputPath> <day>")
      System.exit(1)
    }

    val Array(inputPath, day) = args

    // 关闭类型推导
    val spark = SparkSession.builder().appName("TopNStatJob")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled","false")
      .master("local[2]").getOrCreate()

    val accessDF = spark.read.format("parquet").load(inputPath)

    // 复用已有数据
    import spark.implicits._
    val commonDF = accessDF.filter($"day" === day && $"cmsType" === "video")
    commonDF.cache()


    // 统计前先删除指定数据，确保是唯一的
    StatDAO.deleteData(day)

    //最受欢迎的TopN课程访问次数
    videoAccessTopNStat(spark, commonDF)

    //按照地市进行统计TopN课程
    cityAccessTopNStat(spark, commonDF)

    //按照流量进行统计
    videoTrafficsTopNStat(spark, commonDF)

    commonDF.unpersist(true)
    spark.stop()
  }

  /**
    * 需求一：最受欢迎的Top N课程访问次数
    */
  def videoAccessTopNStat(spark: SparkSession, commonDF:DataFrame): Unit = {

    /**
      * 使用DataFrame的方式进行统计
      */
    import spark.implicits._

    // 先过滤，然后分组，再统计次数，最后降序排序
    val videoAccessTopNDF = commonDF.groupBy("day", "cmsId").agg(count("cmsId").as("times")).orderBy($"times".desc)

    videoAccessTopNDF.show(false)

    /**
      * 使用SQL的方式进行统计
      */
    //    accessDF.createOrReplaceTempView("access_logs")
    //    val videoAccessTopNDF = spark.sql("select day,cmsId, count(1) as times from access_logs " +
    //      "where day='20170511' and cmsType='video' " +
    //      "group by day,cmsId order by times desc")
    //
    //    videoAccessTopNDF.show(false)

    /**
      * 将统计结果写入到MySQL中
      */
    try {
      videoAccessTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoAccessStat]

        // info -> org.apache.spark.sql.Row
        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")

          /**
            * 不建议大家在此处进行数据库的数据插入
            */

          list.append(DayVideoAccessStat(day, cmsId, times))
        })

        // 一个partition插入一次
        StatDAO.insertDayVideoAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }


  /**
    * 需求二：按地市统计最受欢迎TOP N
    */
  def cityAccessTopNStat(spark:SparkSession, commonDF:DataFrame):Unit = {

    import  spark.implicits._
    val cityAccessTopNDF = commonDF.groupBy("day","city","cmsId")
      .agg(count("cmsId").as("times"))

    //cityAccessTopNDF.show(false)
    // 20170511|广东省|14623|11226


    // Window函数在Spark SQL的使用
    val top3DF = cityAccessTopNDF.select(
      cityAccessTopNDF("day"),
      cityAccessTopNDF("city"),
      cityAccessTopNDF("cmsId"),
      cityAccessTopNDF("times"),
      row_number().over(Window.partitionBy(cityAccessTopNDF("city"))
        .orderBy(cityAccessTopNDF("times").desc)
      ).as("times_rank")
    ).filter("times_rank <=3")
//      .show(false)

    // 每个地市Top3
    // 20170511|北京市|14540|22270|1
    // 20170511|北京市|4600|11271|2
    // 20170511|北京市|14390|11175|3
    // 20170511|广东省|14540|22115|1
    // 20170511|广东省|14623|11226|2
    // 20170511|广东省|14704|11216|3


    /**
      * 将统计结果写入到MySQL中
      */
    try {
      top3DF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayCityVideoAccessStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val city = info.getAs[String]("city")
          val times = info.getAs[Long]("times")
          val timesRank = info.getAs[Int]("times_rank")
          list.append(DayCityVideoAccessStat(day, cmsId, city, times, timesRank))
        })

        StatDAO.insertDayCityVideoAccessTopN(list)
      })
    } catch {
      case e:Exception => e.printStackTrace()
    }
  }


  /**
    * 需求三：按照流量进行统计
    */
  def videoTrafficsTopNStat(spark: SparkSession, commonDF:DataFrame): Unit = {
    import spark.implicits._

    val cityAccessTopNDF = commonDF.agg(sum("traffic").as("traffics"))
      .orderBy($"traffics".desc)
    //.show(false)

    /**
      * 将统计结果写入到MySQL中
      */
    try {
      cityAccessTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoTrafficsStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val traffics = info.getAs[Long]("traffics")
          list.append(DayVideoTrafficsStat(day, cmsId,traffics))
        })

        StatDAO.insertDayVideoTrafficsAccessTopN(list)
      })
    } catch {
      case e:Exception => e.printStackTrace()
    }

  }
}


