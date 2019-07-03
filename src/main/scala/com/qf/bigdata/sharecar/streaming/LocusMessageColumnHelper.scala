package com.qf.bigdata.sharecar.streaming

import com.qf.bigdata.sharecar.constant.{CommonConstant, ShareCarConstant}
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.get_json_object
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

/**
  * 轨迹消息列信息
  */
object LocusMessageColumnHelper {


  /**
    * 输出数据源shema
    * @return
    */
  def getKafkaInputPutDataSchema():StructType={
    val schema = StructType(Seq(
      StructField(CommonConstant.KEY_ORDER_CODE, StringType),
      StructField(CommonConstant.KEY_USER_CODE, StringType),
      StructField(CommonConstant.KEY_VEHICLE_CODE, StringType),
      StructField(CommonConstant.KEY_STATUS, StringType),
      StructField(CommonConstant.KEY_ADCODE, StringType),
      StructField(CommonConstant.KEY_ADDRESS, StringType),
      StructField(CommonConstant.KEY_PROVINCE, StringType),
      StructField(CommonConstant.KEY_DISTRICT, StringType),
      StructField(CommonConstant.KEY_LNG, StringType),
      StructField(CommonConstant.KEY_LAT, StringType),
      StructField(CommonConstant.KEY_GEOHASH, StringType),
      StructField(CommonConstant.KEY_G_SIGNAL, StringType),
      StructField(CommonConstant.KEY_CTTIME, StringType)
    ))
    schema
  }


  /**
    * 轨迹消息列
    * @return
    */
  def selectLocusMessageColumns():ArrayBuffer[String] ={
    val columns = new ArrayBuffer[String]()
    columns.+=("CAST(topic as string) as topic")
    columns.+=("CAST(key as string) as key")
    columns.+=("CAST(value as string) as value")
    columns.+=("timestamp")
//    columns.+=("CAST(value as string) as value")
//    columns.+=("CAST(value as string) as value")
    columns
  }




  /**
    * 输出数据源shema
    * @return
    */
  def getStreamingOutPutColumn():ArrayBuffer[String]={
    //import spark.implicits._
    import org.apache.spark.sql.functions._
    val columns = new ArrayBuffer[String]()
    columns.+=(s"v.${CommonConstant.KEY_ORDER_CODE}")
    columns.+=(s"v.${CommonConstant.KEY_USER_CODE}")
    columns.+=(s"v.${CommonConstant.KEY_VEHICLE_CODE}")
    columns.+=(s"v.${CommonConstant.KEY_STATUS}")
    columns.+=(s"v.${CommonConstant.KEY_ADCODE}")

    columns.+=(s"v.${CommonConstant.KEY_LNG}")
    columns.+=(s"v.${CommonConstant.KEY_LAT}")
    columns.+=(s"v.${CommonConstant.KEY_GEOHASH}")
    columns.+=(s"v.${CommonConstant.KEY_PROVINCE}")
    columns.+=(s"v.${CommonConstant.KEY_DISTRICT}")

    columns.+=(s"v.${CommonConstant.KEY_ADDRESS}")
    columns.+=(s"v.${CommonConstant.KEY_G_SIGNAL}")
    columns.+=(s"v.${CommonConstant.KEY_CTTIME}")
    columns.+=(s"${CommonConstant.KEY_TIMESTAMP}")
    columns.+=(s"from_unixtime(v.${CommonConstant.KEY_CTTIME}/1000,'yyyyMMdd') as ${ShareCarConstant.DEF_PARTITION}")
    columns
  }


  /**
    * 输出数据源shema
    * @return
    */
  def getStreamingOutPutDataSchema():StructType={
    val schema = StructType(Seq(
      StructField(CommonConstant.KEY_ORDER_CODE, StringType),
      StructField(CommonConstant.KEY_USER_CODE, StringType),
      StructField(CommonConstant.KEY_VEHICLE_CODE, StringType),
      StructField(CommonConstant.KEY_STATUS, StringType),
      StructField(CommonConstant.KEY_ADCODE, StringType),
      StructField(CommonConstant.KEY_ADDRESS, StringType),
      StructField(CommonConstant.KEY_PROVINCE, StringType),
      StructField(CommonConstant.KEY_DISTRICT, StringType),
      StructField(CommonConstant.KEY_LNG, StringType),
      StructField(CommonConstant.KEY_LAT, StringType),
      StructField(CommonConstant.KEY_GEOHASH, StringType),
      StructField(CommonConstant.KEY_G_SIGNAL, StringType),
      StructField(CommonConstant.KEY_CTTIME, LongType),
      StructField(CommonConstant.KEY_TIMESTAMP, TimestampType)
    ))
    schema
  }







}
