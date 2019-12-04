package com.qf.bigdata.sharecar.streaming.agg

import java.util.concurrent.TimeUnit

import com.qf.bigdata.sharecar.constant.{CommonConstant, ShareCarConstant}
import com.qf.bigdata.sharecar.streaming.LocusMessageColumnHelper
import com.qf.bigdata.sharecar.streaming.writer.ESForeachWriter
import com.qf.bigdata.sharecar.util.{PropertyUtil, SparkHelper}
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/**
  * 轨迹信息统计
  */
class LocusMessageAggHandler{

}


/**
  * 轨迹定位消息处理(重回kafka)
  */
object LocusMessageAggHandler {

  val LOG :Logger = LoggerFactory.getLogger(LocusMessageAggHandler.getClass)


  /**
    * 定位轨迹信息处理
    */
  def handleLocusMessage4HDFS(spark:SparkSession, appName :String, sourceTopic:String, output:String, groupID:String) :Unit = {
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
      options.+=((ShareCarConstant.SPARK_SSM_OPTIONS_KEY_OFFSETS_STARTING, ShareCarConstant.SPARK_SSTREAMING_STARTINGOFFSETS_EARLIEST))
      //options.+=((ShareCarConstant.SPARK_SSM_OPTIONS_KEY_OFFSETS_STARTING, ShareCarConstant.SPARK_SSTREAMING_STARTINGOFFSETS_LATEST))


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
      val locusDF: DataFrame = kafkaDF.selectExpr(locusColumns:_*)
      locusDF.printSchema()

      //输出hdfs
      //window time:second、minutes、day
      val query = locusDF
        .withWatermark("timestamp","10 second")
        .groupBy(
          $"adcode",
            //$"timestamp",
            window($"timestamp", "30 second", "15 second"),
            $"bdp_day")
        .agg(
          approx_count_distinct($"userCode").alias("user_count"),
          count($"userCode").alias("total_count")
        ).writeStream
        .partitionBy(s"${ShareCarConstant.DEF_PARTITION}")
        .outputMode(OutputMode.Append())
        .trigger(Trigger.ProcessingTime(1,TimeUnit.MINUTES))
        .format("parquet")
        .option(ShareCarConstant.SPARK_SSM_OPTIONS_KEY_PATH, output)
        .option(ShareCarConstant.SPARK_SSM_OPTIONS_KEY_CHECKPOINT, ShareCarConstant.SPARK_SSTREAMING_LOCUS_AGG_CHECKPOINT)
        .start()

      query.explain(true)

      query.awaitTermination()

    }catch{
      case ex:Exception => {
        println(s"LocusMessageAggHandler.handleLocusMessage occur exception：app=[$appName], msg=$ex")
        LOG.error(ex.getMessage, ex)
      }
    }finally {
      println(s"LocusMessageAggHandler.handleLocusMessage End：appName=[${appName}], use=[${System.currentTimeMillis() - begin}]")
    }
  }



  /**
    * 流式数据处理
    * @param appName
    */
  def handleStreamingJobs(appName :String, sourceTopic:String, output:String, groupID:String) :Unit = {
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
        .setAppName(appName)
        .setMaster("local[4]")

      //spark上下文会话
      //spark = SparkHelper.createSpark(sconf)
      spark = SparkHelper.createSparkNotHive(sconf)

      handleLocusMessage4HDFS(spark, appName, sourceTopic, output, groupID)

    }catch{
      case ex:Exception => {
        println(s"LocusMessageAggHandler.handleStreamingJobs occur exception：app=[$appName], msg=$ex")
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

    val appName: String = "qf_sss_sharecar_agg_locus2"
    val sourceTopic:String = CommonConstant.TOPIC_SHARE_CAR_TEST
    val output :String = "/data/sharecar/kafka/locus/agg2/"
    val groupID :String = "qf_sharecar_locus_agg2"

    val begin = System.currentTimeMillis()
    handleStreamingJobs(appName, sourceTopic, output, groupID)
    val end = System.currentTimeMillis()

    println(s"appName=[${appName}], begin=$begin, use=${end-begin}")
  }


}




