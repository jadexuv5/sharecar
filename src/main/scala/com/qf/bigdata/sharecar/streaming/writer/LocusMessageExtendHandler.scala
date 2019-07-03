package com.qf.bigdata.sharecar.streaming.writer

import java.util.concurrent.TimeUnit

import com.qf.bigdata.sharecar.constant.{CommonConstant, ShareCarConstant}
import com.qf.bigdata.sharecar.streaming.LocusMessageColumnHelper
import com.qf.bigdata.sharecar.util.{PropertyUtil, SparkHelper}
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/**
  * 轨迹信息统计
  */
class LocusMessageExtendHandler{

}


/**
  * 轨迹定位消息处理
  */
object LocusMessageExtendHandler {

  val LOG :Logger = LoggerFactory.getLogger(LocusMessageExtendHandler.getClass)


  /**
    * 定位轨迹信息处理
    * spark structured streaming writer
    */
  def handleLocusMessage4ESWriter(spark:SparkSession, appName :String, sourceTopic:String, groupID:String, indexName:String, typeName:String) :Unit = {
    val begin = System.currentTimeMillis()
    try{
      import org.apache.spark.sql.functions._
      import spark.implicits._

      import scala.collection.JavaConverters._

      //数据流读取选项
      val options :collection.mutable.Map[String, String] = PropertyUtil.readProperties2Map4String (CommonConstant.KAFKA_CONFIG_PATH).asScala
      options.+=((ShareCarConstant.SPARK_SSM_OPTIONS_KEY_BOOTSTRAP_SUBSCRIBE,sourceTopic))
      options.+=((ShareCarConstant.SPARK_SSM_OPTIONS_KEY_BOOTSTRAP_GROUPID,groupID))
      options.+=(("failOnDataLoss","false"))
      //options.+=((ShareCarConstant.SPARK_SSM_OPTIONS_KEY_CONSUMER_POLLTIMEOUT_MS, ShareCarConstant.SPARK_SSM_OPTIONS_CONSUMER_POLLTIMEOUT_MS_DEF))
      //最近信息拉取
      //options.+=((ShareCarConstant.SPARK_SSM_OPTIONS_KEY_OFFSETS_STARTING, ShareCarConstant.SPARK_SSTREAMING_STARTINGOFFSETS_EARLIEST))
      options.+=((ShareCarConstant.SPARK_SSM_OPTIONS_KEY_OFFSETS_STARTING, ShareCarConstant.SPARK_SSTREAMING_STARTINGOFFSETS_LATEST))


      //流式数据集
      val locusKafkaSchema: StructType = LocusMessageColumnHelper.getKafkaInputPutDataSchema()
      val kafkaDF :DataFrame = spark.readStream.format("kafka")
        .options(options)
        .load()
        .select(from_json(col("value").cast("string"),locusKafkaSchema).alias("v"),$"timestamp")
      //kafkaDF.printSchema()
      //.select(get_json_object(($"value").cast("string"), "$.zip").alias("zip"))

      //kafka数据解析
      val locusColumns :Seq[String] = LocusMessageColumnHelper.getStreamingOutPutColumn()
      val locusDF = kafkaDF.selectExpr(locusColumns:_*)
      locusDF.printSchema()

      //extend writer
      val esWriter = new ESForeachWriter(indexName, typeName)


      //输出hdfs
      //window time:second、minutes、day
      val query = locusDF
        .writeStream
        .foreach(esWriter)
        .outputMode(OutputMode.Append())
        //.trigger(Trigger.ProcessingTime(1,TimeUnit.MINUTES))
        .option(ShareCarConstant.SPARK_SSM_OPTIONS_KEY_CHECKPOINT, ShareCarConstant.SPARK_SSTREAMING_LOCUS_ES_CHECKPOINT)
        .start()

      //query.explain(true)

      query.awaitTermination()

    }catch{
      case ex:Exception => {
        println(s"LocusMessageExtendHandler.handleLocusMessage4ESWriter occur exception：app=[$appName], msg=$ex")
        LOG.error(ex.getMessage, ex)
      }
    }finally {
      println(s"LocusMessageExtendHandler.handleLocusMessage4ESWriter End：appName=[${appName}], use=[${System.currentTimeMillis() - begin}]")
    }
  }


  /**
    * 定位轨迹信息处理
    * elasticsearch-spark-20_2.11 三方包处理
    */
  def handleLocusMessage4ES(spark:SparkSession, appName :String, sourceTopic:String, groupID:String, indexName:String, typeName:String) :Unit = {
    val begin = System.currentTimeMillis()
    try{
      import org.apache.spark.sql.functions._
      import spark.implicits._

      import scala.collection.JavaConverters._

      //数据流读取选项
      val options :collection.mutable.Map[String, String] = PropertyUtil.readProperties2Map4String (CommonConstant.KAFKA_CONFIG_PATH).asScala
      options.+=((ShareCarConstant.SPARK_SSM_OPTIONS_KEY_BOOTSTRAP_SUBSCRIBE,sourceTopic))
      options.+=((ShareCarConstant.SPARK_SSM_OPTIONS_KEY_BOOTSTRAP_GROUPID,groupID))
      //针对流式场景丢失数据情况配置
      options.+=(("failOnDataLoss","false"))
      //options.+=((ShareCarConstant.SPARK_SSM_OPTIONS_KEY_CONSUMER_POLLTIMEOUT_MS, ShareCarConstant.SPARK_SSM_OPTIONS_CONSUMER_POLLTIMEOUT_MS_DEF))
      //最近信息拉取
      //options.+=((ShareCarConstant.SPARK_SSM_OPTIONS_KEY_OFFSETS_STARTING, ShareCarConstant.SPARK_SSTREAMING_STARTINGOFFSETS_EARLIEST))
      options.+=((ShareCarConstant.SPARK_SSM_OPTIONS_KEY_OFFSETS_STARTING, ShareCarConstant.SPARK_SSTREAMING_STARTINGOFFSETS_LATEST))


      //流式数据集
      val locusKafkaSchema: StructType = LocusMessageColumnHelper.getKafkaInputPutDataSchema()
      val kafkaDF :DataFrame = spark.readStream.format("kafka")
        .options(options)
        .load()
        .select(from_json(col("value").cast("string"),locusKafkaSchema).alias("v"),$"timestamp")
      //kafkaDF.printSchema()
      //.select(get_json_object(($"value").cast("string"), "$.zip").alias("zip"))

      //kafka数据解析
      val locusColumns :Seq[String] = LocusMessageColumnHelper.getStreamingOutPutColumn()
      val locusDF = kafkaDF.selectExpr(locusColumns:_*)
      locusDF.printSchema()

      //extend
      val esWriter = new ESForeachWriter(indexName, typeName)

      val indexType = indexName+"/"+typeName

      //输出hdfs
      //window time:second、minutes、day
      val query = locusDF
        .writeStream
        .foreach(esWriter)
        .outputMode(OutputMode.Append())
        .format(ShareCarConstant.SPARK_FORMAT_EXTEND_ES)
        .option(ShareCarConstant.SPARK_SSM_OPTIONS_KEY_CHECKPOINT, ShareCarConstant.SPARK_SSTREAMING_LOCUS_ES_CHECKPOINT)
        .start(indexType)

      //query.explain(true)

      query.awaitTermination()

    }catch{
      case ex:Exception => {
        println(s"LocusMessageExtendHandler.handleLocusMessage4ES occur exception：app=[$appName], msg=$ex")
        LOG.error(ex.getMessage, ex)
      }
    }finally {
      println(s"LocusMessageExtendHandler.handleLocusMessage4ES End：appName=[${appName}], use=[${System.currentTimeMillis() - begin}]")
    }
  }


  /**
    * 流式数据处理
    * @param appName
    */
  def handleStreamingJobs(appName :String, sourceTopic:String, groupID:String,indexName:String, typeName:String) :Unit = {
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
        //.set("spark.sql.streaming.checkpointLocation",ShareCarConstant.SPARK_SSTREAMING_CHECKPOINT)
        .set("spark.sql.streaming.schemaInference","true")
        .set("spark.cleaner.referenceTracking.cleanCheckpoints","false")
        .set("es.index.auto.create","true")
        .set("es.nodes", "node242,node243,node244")
        .set("es.port", "9200")
        .set("es.nodes.wan.only", "true")
        .setAppName(appName)
        .setMaster("local[4]")

      //spark上下文会话
      //spark = SparkHelper.createSpark(sconf)
      spark = SparkHelper.createSparkNotHive(sconf)

      //数据流式采集写入外部数据源ES
      //handleLocusMessage4ES(spark, appName, sourceTopic, groupID, indexName, typeName)

      //spark stuctured streaming 外部数据源写入方式
      handleLocusMessage4ESWriter(spark, appName, sourceTopic, groupID, indexName, typeName)

    }catch{
      case ex:Exception => {
        println(s"LocusMessageExtendHandler.handleStreamingJobs occur exception：app=[$appName], msg=$ex")
        LOG.error(ex.getMessage, ex)
      }
    }finally {
      if(spark != null){
        spark.stop()
      }
    }
  }


  def main(args: Array[String]): Unit = {

    //val Array(appName, topic, output, groupID) = args

    val appName: String = "qf_sss_sharecar_locus_es_writer"
    val sourceTopic:String = CommonConstant.TOPIC_SHARE_CAR_TEST
    //val output :String = "/data/sharecar/kafka/locus/agg2/"
    val groupID :String = "qf_sss_sharecar_locus_extend_writer_es"
    val indexName :String = "locus_agg"
    val typeName :String = "locus_agg"

    val begin = System.currentTimeMillis()
    handleStreamingJobs(appName, sourceTopic, groupID, indexName, typeName)
    val end = System.currentTimeMillis()

    println(s"appName=[${appName}], begin=$begin, use=${end-begin}")
  }


}