package com.qf.bigdata.sharecar.streaming.agg

/**
  * 这里先建立dw层进行过滤广告时长小于5秒的
  */

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer

object DWVideo {

  def handleJobs(appName:String,bdp_day:String): Unit = {
    try{
      val sparkConf: SparkConf = new SparkConf()
        .set("hive.exec.dynamic.partition", "true")
        .set("hive.exec.dynamic.partition.mode", "nonstrict")
        .set("spark.sql.shuffle.partitions", "32")
        .set("hive.merge.mapfiles", "true")
        .set("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
        .set("spark.sql.autoBroadcastJoinThreshold", "50485760")
        .set("spark.sql.crossJoin.enabled", "true")
        .setMaster("local[*]")
        .setAppName(appName)

      val spark: SparkSession = SparkSession
        .builder()
        .config(sparkConf)
        .enableHiveSupport()
        .getOrCreate()
      import spark.implicits._

      val odsVideoDF: DataFrame = spark.read.table("ods_video.ods_01_videolog")

      val columns = new ArrayBuffer[String]()
      columns.+=("video_type")
      columns.+=("video_duration")
      columns.+=("video_duration_play")
      columns.+=("ad_type")
      columns.+=("ad_duration")
      columns.+=("ad_duration_play")
      columns.+=("video_produced_area")
      columns.+=("video_produced_time")
      columns.+=("video_name")
      columns.+=("user")
      columns.+=("ct")
      columns.+=("bdp_day")

      import org.apache.spark.sql.functions._

      //放DW层进行过滤条件 去除广告时间小于5秒
      val condition = (col("bdp_day")===lit(bdp_day) and col("")===lit("01"))
      val selectDF: DataFrame = odsVideoDF.selectExpr(columns:_*)
        .where(col("bdp_day")===lit(bdp_day) and col("ad_duration_play").gt(5))
          .repartition(4)

      //select出的表存储到hive dw层表中
      selectDF.write.mode(saveMode = SaveMode.Overwrite).insertInto("jade_dw_video.dw_video_playinfo")








    } catch {
      case ex: Exception =>{
        ex.printStackTrace()
      }
    }

  }

  def main(args: Array[String]): Unit = {
    val appName:String = "dm_video_job"
    val bdp_day = "20191114"
    val beginTime = System.currentTimeMillis()
    handleJobs(appName,bdp_day)
    val endTime = System.currentTimeMillis()
    println("sumTime = " + (endTime-beginTime)/1000 + "s")

  }
}
