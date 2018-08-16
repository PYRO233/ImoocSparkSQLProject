package com.imooc.spark_log_analysis.utils

import IpUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
  * 访问日志转换(输入==>输出)工具类
  * 输入：访问时间、访问URL、耗费的流量、访问IP地址信息
  * 输出：URL、cmsType(video/article)、cmsId(编号)、流量、ip、城市信息、访问时间、天
  */
object AccessConvertUtil {
  // org.apache.spark.sql.types.StructType
  val struct = StructType(
    Array(
      StructField("url",StringType),
      StructField("cmsType",StringType),
      StructField("cmsId",LongType),
      StructField("traffic",LongType),
      StructField("ip",StringType),
      StructField("city",StringType),
      StructField("time",StringType),
      StructField("day",StringType)
    )
  )

  /**
    * 根据输入的每一行信息转换成输出的样式
    * @param log  输入的每一行记录信息
    */
  def parseLog(log:String) = {

    try{
      val splits = log.split("\t")

      val url = splits(1)
      val traffic = splits(2).toLong
      val ip = splits(3)

      val domain = "http://www.imooc.com/"
      val cms = url.substring(url.indexOf(domain) + domain.length)
      val cmsTypeId = cms.split("/")

      var cmsType = ""
      var cmsId = 0l
      if(cmsTypeId.length > 1) {
        cmsType = cmsTypeId(0)
        cmsId = cmsTypeId(1).toLong
      }

      val city = IpUtils.getCity(ip)
//      val city = "未知"
      val time = splits(0)
      val day = time.substring(0,10).replaceAll("-","")


      // Row里面的字段要和Struct中的字段对应上
      Row(url, cmsType, cmsId, traffic, ip, city, time, day)
    } catch {
      case e:Exception => Row(0)
    }
  }
}
