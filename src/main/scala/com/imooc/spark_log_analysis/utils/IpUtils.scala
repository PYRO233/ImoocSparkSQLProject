package com.imooc.spark_log_analysis.utils

/**
  * 借助github开源项目
  */
object IpUtils {
  def getCity(ip:String) = {
//    IpHelper.findRegionByIp(ip)
  }

  def main(args: Array[String]) {
    println(getCity("218.75.35.226"))
  }
}
