package com.qf.bigdata.sharecar.streaming.agg

/**
  * 先从ods层过滤数据到dw层
  * dm层读取dw层过滤好的数据，进行聚合
  */

import org.apache.spark.SparkConf
import org.apache.spark.sql._

import scala.collection.mutable.ArrayBuffer

object DMVideo {
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

      val dwVideoDF: DataFrame = spark.read.table("jade_dw_video.dw_video_playinfo")

      val columns = new ArrayBuffer[String]()
      columns.+=("video_type")
      columns.+=("video_produced_area")
      columns.+=("substr(video_produced_time,3,1) as video_produced_period")
      columns.+=("total_video_play")
      columns.+=("total_ad_play")
      columns.+=("total_uv")
      columns.+=("total_pv")
      columns.+=("bdp_day")

      import org.apache.spark.sql.functions._

      val selectDF: Dataset[Row] = dwVideoDF
        .groupBy(
          "video_type",
          "video_produced_area",
          "video_produced_period"
        )
        .agg(
          sum("video_duration_play").alias("total_video_play"),
          sum("ad_duration_play").alias("total_ad_play"),
          approx_count_distinct("user_id").alias("total_uv"),
          count("user_id").alias("total_pv")
        ).selectExpr(columns: _*).where(col("bdp_day") === lit(bdp_day))

      //select出的表存储到hive dm层表中(表建好了名字不改了）
      selectDF.write.mode(saveMode = SaveMode.Overwrite).insertInto("jade_dm_video.ods_video_playinfo")

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
