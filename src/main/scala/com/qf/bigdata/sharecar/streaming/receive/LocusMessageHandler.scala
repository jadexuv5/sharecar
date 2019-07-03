package com.qf.bigdata.sharecar.streaming.receive

import com.jayway.jsonpath.JsonPath
import com.qf.bigdata.sharecar.constant.{CommonConstant, ShareCarConstant}
import com.qf.bigdata.sharecar.streaming.LocusMessageColumnHelper
import com.qf.bigdata.sharecar.util.{PropertyUtil, SparkHelper}
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

class LocusMessageHandler{

}

/**
  * 轨迹定位消息处理
  */
object LocusMessageHandler {

  val LOG :Logger = LoggerFactory.getLogger(LocusMessageHandler.getClass)


  /**
    * 定位轨迹信息处理
    */
  def handleLocusMessageRDD(spark:SparkSession, appName :String, topic:String, groupID:String, bdpDAY:String, output:String) :Unit = {
    val begin = System.currentTimeMillis()
    try{
      import scala.collection.JavaConverters._

      //数据流读取选项
      val options :collection.mutable.Map[String, String] = PropertyUtil.readProperties2Map4String (CommonConstant.KAFKA_CONFIG_PATH).asScala
      options.+=((ShareCarConstant.SPARK_SSM_OPTIONS_KEY_BOOTSTRAP_SUBSCRIBE,topic))
      options.+=((ShareCarConstant.SPARK_SSM_OPTIONS_KEY_BOOTSTRAP_GROUPID,groupID))
      options.+=((ShareCarConstant.SPARK_SSM_OPTIONS_KEY_CONSUMER_POLLTIMEOUT_MS, ShareCarConstant.SPARK_SSM_OPTIONS_CONSUMER_POLLTIMEOUT_MS_DEF))

      //最近信息拉取
      options.+=((ShareCarConstant.SPARK_SSM_OPTIONS_KEY_OFFSETS_STARTING, ShareCarConstant.SPARK_SSTREAMING_STARTINGOFFSETS_EARLIEST))
      //options.+=((ShareCarConstant.SPARK_SSM_OPTIONS_KEY_OFFSETS_STARTING, ShareCarConstant.SPARK_SSTREAMING_STARTINGOFFSETS_LATEST))


      //流式数据集
      val locusColumns = LocusMessageColumnHelper.selectLocusMessageColumns
      val locusDF :DataFrame = spark.readStream.format("kafka")
        .options(options)
        .load()
      locusDF.printSchema()


      //转换topic里的信息
      val mapLocusDF :DataFrame = locusDF.selectExpr(locusColumns:_*)
      mapLocusDF.printSchema()


      //定位信息输出schema
      val locusDataSchema :StructType =  LocusMessageColumnHelper.getStreamingOutPutDataSchema()
      //定位信息数据源encoder
      val locusDataEncoder = RowEncoder(locusDataSchema)
      val nLocusDF = mapLocusDF.map(row =>{
        val kafkaInfo = row.mkString(",")
        LOG.info("kafkaInfo=", kafkaInfo)

        val topic = row.getAs[String]("topic")
        val key = row.getAs[String]("key")
        val value = row.getAs[String]("value")
        LOG.info("topic={},key={},value={}", topic, key, value)

        //定位信息
        val orderCode: String = JsonPath.parse(value).read("orderCode")
        val userCode: String = JsonPath.parse(value).read("userCode")
        val vehicleCode: String = JsonPath.parse(value).read("vehicleCode")
        val status: String = JsonPath.parse(value).read("status")
        val district: String = JsonPath.parse(value).read("district")
        val province: String = JsonPath.parse(value).read("province")
        val adcode: String = JsonPath.parse(value).read("adcode")
        val addr: String = JsonPath.parse(value).read("addr")
        val latitude: String = JsonPath.parse(value).read("latitude")
        val longitude: String = JsonPath.parse(value).read("longitude")
        val geoHash: String = JsonPath.parse(value).read("geoHash")
        val gSignal: String = JsonPath.parse(value).read("gSignal")
        val ctTime: AnyRef = JsonPath.parse(value).read("ctTime")

        RowFactory.create(orderCode, userCode, vehicleCode, status, province, adcode, district,
          addr, latitude, longitude,geoHash, gSignal,ctTime)
      })(locusDataEncoder)
      nLocusDF.printSchema()


      //输出hdfs
      var noutput = output
      if(output.lastIndexOf(CommonConstant.PATH_L) != output.length-1){
        noutput = output + CommonConstant.PATH_L
      }
      val partitionOutPath = noutput+ShareCarConstant.DEF_PARTITION+ShareCarConstant.DEF_PARTITION_JOINSIGN+bdpDAY+CommonConstant.PATH_L
      val query = nLocusDF.writeStream
        .format("parquet")
        .option(ShareCarConstant.SPARK_SSM_OPTIONS_KEY_PATH, partitionOutPath)
        .option(ShareCarConstant.SPARK_SSM_OPTIONS_KEY_CHECKPOINT, ShareCarConstant.SPARK_SSTREAMING_LOCUS_ODS_CHECKPOINT)
        .outputMode(OutputMode.Append())
        .start()

      query.awaitTermination()

    }catch{
      case ex:Exception => {
        println(s"LocusMessageHandler.handleLocusMessage occur exception：app=[$appName], msg=$ex")
        LOG.error(ex.getMessage, ex)
      }
    }finally {
      println(s"LocusMessageHandler.handleLocusMessage End：appName=[${appName}], use=[${System.currentTimeMillis() - begin}]")
    }
  }


  /**
    * 定位轨迹信息处理
    */
  def handleLocusMessageDF(spark:SparkSession, appName :String, topic:String, output:String, groupID:String) :Unit = {
    val begin = System.currentTimeMillis()
    try{
      import org.apache.spark.sql.functions._
      import spark.implicits._

      import scala.collection.JavaConverters._

      //数据流读取选项
      val options :collection.mutable.Map[String, String] = PropertyUtil.readProperties2Map4String (CommonConstant.KAFKA_CONFIG_PATH).asScala
      options.+=((ShareCarConstant.SPARK_SSM_OPTIONS_KEY_BOOTSTRAP_SUBSCRIBE,topic))
      options.+=((ShareCarConstant.SPARK_SSM_OPTIONS_KEY_BOOTSTRAP_GROUPID,groupID))
      options.+=(("failOnDataLoss","false"))
      //options.+=((ShareCarConstant.SPARK_SSM_OPTIONS_KEY_OFFSETS_STARTING, ShareCarConstant.SPARK_SSTREAMING_STARTINGOFFSETS_EARLIEST))
      //options.+=((ShareCarConstant.SPARK_SSM_OPTIONS_KEY_CONSUMER_POLLTIMEOUT_MS, ShareCarConstant.SPARK_SSM_OPTIONS_CONSUMER_POLLTIMEOUT_MS_DEF))

      //最近信息拉取
      options.+=((ShareCarConstant.SPARK_SSM_OPTIONS_KEY_OFFSETS_STARTING, ShareCarConstant.SPARK_SSTREAMING_STARTINGOFFSETS_LATEST))

      //流式数据集
      val locusKafkaSchema: StructType = LocusMessageColumnHelper.getKafkaInputPutDataSchema()
      val kafkaDF :DataFrame = spark.readStream.format("kafka")
        .options(options)
        .load()
        .select(from_json(col("value").cast("string"),locusKafkaSchema).alias("v"),$"timestamp")
      kafkaDF.printSchema()

      //kafka数据解析
      val locusColumns :Seq[String] = LocusMessageColumnHelper.getStreamingOutPutColumn()
      val locusDF = kafkaDF.selectExpr(locusColumns:_*)
      locusDF.printSchema()


      //输出hdfs
      var noutput = output
      if(output.lastIndexOf(CommonConstant.PATH_L) != output.length-1){
        noutput = output + CommonConstant.PATH_L
      }
      val query = locusDF.writeStream
        .partitionBy(s"${ShareCarConstant.DEF_PARTITION}")
        .format("parquet")
        .option(ShareCarConstant.SPARK_SSM_OPTIONS_KEY_PATH, noutput)
        .option(ShareCarConstant.SPARK_SSM_OPTIONS_KEY_CHECKPOINT, ShareCarConstant.SPARK_SSTREAMING_LOCUS_ODS_CHECKPOINT)
        .outputMode(OutputMode.Append())
        .start()

      query.awaitTermination()

    }catch{
      case ex:Exception => {
        println(s"LocusMessageHandler.handleLocusMessage occur exception：app=[$appName], msg=$ex")
        LOG.error(ex.getMessage, ex)
      }
    }finally {
      println(s"LocusMessageHandler.handleLocusMessage End：appName=[${appName}], use=[${System.currentTimeMillis() - begin}]")
    }
  }



  /**
    * 流式数据处理
    * @param appName
    */
  def handleStreamingJobs(appName :String, topic:String, output:String, groupID:String) :Unit = {
    var spark :SparkSession = null
    try{
      //spark配置参数
      val sconf = new SparkConf()
        .set("hive.exec.dynamic.partition", "true")
        .set("hive.exec.dynamic.partition.mode", "nonstrict")
        .set("spark.sql.shuffle.partitions", "32")
        .set("hive.merge.mapfiles", "true")
        .set("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
        .set("spark.sql.autoBroadcastJoinThreshold", "50485760")
        .set("spark.sql.crossJoin.enabled", "true")

        //.set("spark.sql.streaming.checkpointLocation",ShareCarConstant.SPARK_SSTREAMING_LOCUS_CHECKPOINT)
        .setAppName(appName)
        .setMaster("local[4]")

      //spark上下文会话
      //spark = SparkHelper.createSpark(sconf)
      spark = SparkHelper.createSparkNotHive(sconf)


      handleLocusMessageDF(spark, appName, topic, output, groupID)


    }catch{
      case ex:Exception => {
        println(s"LocusMessageHandler.handleStreamingJobs occur exception：app=[$appName], msg=$ex")
        LOG.error(ex.getMessage, ex)
      }
    }finally {
      if(spark != null){
        spark.stop()
      }
    }
  }




  def main(args: Array[String]): Unit = {

    //val Array(appName, groupID, topic, output) = args

    val appName: String = "qf_sss_sharecar_locus_ods"
    val output :String = "/data/sharecar/kafka/locus/ods/"
    val groupID :String = "qf_sss_sharecar_locus_ods"
    val topic :String = CommonConstant.TOPIC_SHARE_CAR_TEST

    val begin = System.currentTimeMillis()
    handleStreamingJobs(appName,topic, output, groupID)
    val end = System.currentTimeMillis()

    println(s"appName=[${appName}], begin=$begin, use=${end-begin}")
  }


}


