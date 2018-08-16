package com.imooc.spark_log_analysis.domain

/**
  * 每天课程访问次数实体类
  */
case class DayVideoAccessStat(day:String, cmsId:Long, times:Long)
