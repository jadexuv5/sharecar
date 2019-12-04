package com.qf.bigdata.sharecar.streaming.agg
import java.util.concurrent.TimeUnit

import com.qf.bigdata.sharecar.streaming.agg.LocusMessageAggHandler.LOG
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer
object Video {

  def main(args: Array[String]): Unit = {

    val appName = "video"
    val conf = new SparkConf()
      .set("hive.exec.dynamic.partition", "true")
      .set("hive.exec.dynamic.partition.mode", "nonstrict")
      .set("spark.sql.shuffle.partitions", "32")
      .set("hive.merge.mapfiles", "true")
      .set("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
      .set("spark.sql.autoBroadcastJoinThreshold", "50485760")
      .set("spark.sql.crossJoin.enabled", "true")
      //.set("spark.sql.streaming.checkpointLocation",ShareCarConstant.SPARK_SSTREAMING_CHECKPOINT)
      .set("spark.sql.streaming.schemaInference", "true")
      .set("spark.cleaner.referenceTracking.cleanCheckpoints", "false")
      .setAppName("agg")
      .setMaster("local[4]")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()


    try {

      val Schema = StructType(Seq(
        StructField("video_type", StringType),
        StructField("video_duration", StringType),
        StructField("video_duration_play", StringType),
        StructField("ad_type", StringType),
        StructField("ad_duration", StringType),
        StructField("ad_duration_play", StringType),
        StructField("video_produced_area", StringType),
        StructField("video_produced_time", StringType),
        StructField("video_name", StringType),
        StructField("user", StringType),
        StructField("ct", StringType)
      ))

      import spark.implicits._
      val kafkaDF: DataFrame = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "node242:9092,node243:9092,node244:9092")
        .option("zookeeper.connect", "node242:2181,node243:2181,node244:2181/kafka")
        .option("subscribe", "t_week_1901")
        .option("group.id", "video")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        .select(from_json(col("value").cast("string"), Schema).alias("v"), $"timestamp")

      val columns = new ArrayBuffer[String]()
      columns.+=("v.video_type")
      columns.+=("v.video_duration")
      columns.+=("v.video_duration_play")
      columns.+=("v.ad_type")
      columns.+=("v.ad_duration")
      columns.+=("v.ad_duration_play")
      columns.+=("v.video_produced_area")
      columns.+=("v.video_produced_time")
      columns.+=("v.video_name")
      columns.+=("v.user")
      columns.+=("v.ct")
      columns.+=("from_unixtime(v.ct/1000,'yyyy-MM-dd') as bdp_day")

      val locusDF = kafkaDF.selectExpr(columns: _*)
      locusDF.printSchema()


      val query = locusDF
//        .withWatermark("ct", "10 second")
        .writeStream
        .partitionBy("bdp_day")
        .outputMode(OutputMode.Append())
        .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
        .format("parquet")
        .option("path", "/jadedata/video/ods/play_info1/")
        .option("checkpointLocation", "/jadedata/video/ods/zk1")
        .start()

      query.awaitTermination()

    } catch {
      case ex: Exception => {
        println(s"LocusMessageAggHandler.handleLocusMessage occur exception：app=[$appName], msg=$ex")
        LOG.error(ex.getMessage, ex)
      }
    } finally {
      println(s"LocusMessageAggHandler.handleLocusMessage End：appName=[${appName}]")
    }

  }
}
