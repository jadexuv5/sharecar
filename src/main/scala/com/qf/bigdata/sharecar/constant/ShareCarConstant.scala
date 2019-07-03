package com.qf.bigdata.sharecar.constant

import org.apache.spark.sql.SaveMode
import org.apache.spark.storage.StorageLevel

object ShareCarConstant {

  //jdbc config
  val JDBC_CONFIG_PATH = "jdbc.properties"
  val JDBC_CONFIG_USER = "user"
  val JDBC_CONFIG_PASS = "password"
  val JDBC_CONFIG_URL = "url"

  //同步数据(mysql->hive)状态值
  val SYNCDATA_USED :Int = 1
  val SYNCDATA_NOT_USED :Int = 0
  val SYNCDATA_MYSQL_HIVE :String = "release.sync_mysql_hive"

  val MYSQL_2_HIVE:String = "m2h"
  val HIVE_2_MYSQL:String = "h2m"

  //partition
  val DEF_PARTITIONS_FACTOR = 4
  val DEF_FILEPARTITIONS_FACTOR = 10
  val DEF_SOURCE_PARTITIONS = 4
  val DEF_OTHER_PARTITIONS = 8
  val DEF_STORAGE_LEVEL :StorageLevel= StorageLevel.MEMORY_AND_DISK
  val DEF_SAVEMODE:SaveMode  = SaveMode.Overwrite
  val DEF_PARTITION:String = "bdp_day"
  val DEF_PARTITION_JOINSIGN:String = "="

  //spark 3th source format
  val SPARK_FORMAT_EXTEND_ES = "org.elasticsearch.spark.sql"


  //structured streaming
  //val SPARK_SSTREAMING_CHECKPOINT: String = "/spark-structured-streaming/checkpoint/sharecar"
  val SPARK_SSTREAMING_LOCUS_ODS_CHECKPOINT: String = "/spark-structured-streaming/checkpoint/sharecar/locus/ods/"
  val SPARK_SSTREAMING_LOCUS_DM_CHECKPOINT: String = "/spark-structured-streaming/checkpoint/sharecar/locus/dm/"
  val SPARK_SSTREAMING_LOCUS_AGG_CHECKPOINT: String = "/spark-structured-streaming/checkpoint/sharecar/locus/agg/"

  val SPARK_SSTREAMING_LOCUS_ES_CHECKPOINT: String = "/spark-structured-streaming/checkpoint/sharecar/locus/es/"


  val SPARK_SSM_OPTIONS_KEY_CHECKPOINT :String = "checkpointLocation"
  val SPARK_SSM_OPTIONS_KEY_PATH :String = "path"
  val SPARK_SSM_OPTIONS_KEY_BOOTSTRAP_SERVERS :String = "kafka.bootstrap.servers"
  val SPARK_SSM_OPTIONS_KEY_BOOTSTRAP_GROUPID :String = "group.id"
  val SPARK_SSM_OPTIONS_KEY_BOOTSTRAP_SUBSCRIBE :String = "subscribe"
  val SPARK_SSM_OPTIONS_KEY_BOOTSTRAP_TOPIC :String = "topic"
  val SPARK_SSM_OPTIONS_KEY_BOOTSTRAP_SUBSCRIBE_PATTERN :String = "subscribePattern"

  /**
    * option("startingOffsets", """{"topic1":{"0":23,"1":-2},"topic2":{"0":-2}}""")
    * option("assign", """{"topic1":{"0":23,"1":-2},"topic2":{"0":-2}}""")
    */
  val SPARK_SSM_OPTIONS_KEY_OFFSETS_STARTING :String = "startingOffsets"
  val SPARK_SSM_OPTIONS_KEY_OFFSETS_ENDING :String = "endingOffsets"
  val SPARK_SSM_OPTIONS_KEY_ASSIGN :String = "assign"

  //拉取kafka时间
  val SPARK_SSM_OPTIONS_KEY_CONSUMER_POLLTIMEOUT_MS :String = "kafkaConsumer.pollTimeoutMs"

  val SPARK_SSM_OPTIONS_CONSUMER_POLLTIMEOUT_MS_DEF :String = "512"

  val SPARK_SSM_OPTIONS_KEY_FETCHOFFSET_RETRYINTERVAL_MS :String = "fetchOffset.retryIntervalMs"


  val SPARK_SSTREAMING_STARTINGOFFSETS_EARLIEST :String = "earliest"
  val SPARK_SSTREAMING_STARTINGOFFSETS_LATEST :String = "latest"


  //==Column========================================================

  val STREAMING_COL_UT :String = "ut"
  val STREAMING_COL_ADCODE :String = "adcode"
  val STREAMING_COL_USER_COUNT :String = "user_count"
  val STREAMING_COL_TOTAL_COUNT :String = "total_count"

}
