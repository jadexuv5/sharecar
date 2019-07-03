package com.qf.bigdata.sharecar.streaming.writer


import com.fasterxml.jackson.databind.ObjectMapper
import com.jayway.jsonpath.JsonPath
import com.qf.bigdata.sharecar.constant.ShareCarConstant
import com.qf.bigdata.sharecar.util.es.{ES6ClientUtil, ESConfigUtil}
import com.qf.bigdata.sharecar.util.es.ESConfigUtil.ESConfig
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{ForeachWriter, Row}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.script.{Script, ScriptType}
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
  * SparkStructuredStreaming ES Writer
  * @param esConfigPath
  * @param indexName
  * @param typeName
  */
class ESForeachWriter(indexName:String, typeName:String) extends ForeachWriter[Row]{

  val logger :Logger = LoggerFactory.getLogger(this.getClass)

  /**
    * es配置文件路径
    */
  val esConfigPath = "es/es-config.json"


  /**
    * 重试次数
    */
  val retryNumber = 10


  /**
    * es连接上下文
    */
  var transportClient: TransportClient = null



  /**
    * es集群连接
    * @param partitionId
    * @param version
    * @return
    */
  override def open(partitionId: Long, version: Long): Boolean = {
    var result = true
    val begin = System.currentTimeMillis()
    try{
         System.setProperty("es.set.netty.runtime.available.processors", "false")
        transportClient = ES6ClientUtil.buildTransportClient(esConfigPath)
    }catch {
      case ex: Exception => {
        println(s"ESForeachWriter.open occur exception： msg=$ex")
        logger.error(ex.getMessage, ex)
        result = false
      }
    } finally {
      println(s"ESForeachWriter.open ：use=[${System.currentTimeMillis() - begin}]")
    }
    result
  }


  /**
    * 数据处理
    * @param value
    */
  override def process(value: Row): Unit = {

    print(value)
    val esID: String = value.getAs[String]("adcode")


    var valueMap :mutable.Map[String, Object] = mutable.Map[String, Object]()

    val vstruct:StructType = value.schema
    for(fieldName <- vstruct.fieldNames if !fieldName.equalsIgnoreCase("timestamp")){
       val fieldValue = value.getAs[AnyRef](fieldName)
       valueMap.put(fieldName, fieldValue)
    }

    handlerData(indexName, typeName, esID, valueMap)

  }

  /**
    * 变更列
    * @return
    */
  def getAllChangeColKey() : List[String] = {
    var allChangeColKey :List[String] = List[String]()
    allChangeColKey = allChangeColKey.:+(ShareCarConstant.STREAMING_COL_USER_COUNT)
    allChangeColKey = allChangeColKey.:+(ShareCarConstant.STREAMING_COL_TOTAL_COUNT)
    allChangeColKey
  }


  /**
    * 数据处理
    * @param idxName
    * @param typeName
    * @param esID
    * @param value
    * @param currentTime
    */
  def handlerData(idxName :String, typeName :String, esID :String,
                  value: mutable.Map[String, Object]): Unit ={
    val params = new java.util.HashMap[String, Object]

    val scriptSb: StringBuilder = new StringBuilder
    for ((key, value) <- value) {
      params.put(key, value)

      var s = ""
      if(ShareCarConstant.STREAMING_COL_UT.equals(key)) {
        //时间更新
        s = "if(ctx._source."+key+" == null){ctx._source."+key+" = params."+key+"} else { if(ctx._source."+key+" < params."+key+" ){ctx._source."+key+" = params."+key+"}}"
      }else if(getAllChangeColKey().contains(key)){
        //次数累计
        s = "if(ctx._source."+key+" != null) {ctx._source."+key+" += params." + key + "} else { ctx._source."+key+" = params."+key+"} "
      }else {
        s = "if(ctx._source."+key+" == null){ctx._source."+key+" = params."+key+"}"
      }
      scriptSb.append(s)
    }

    val scripts = scriptSb.toString()
    val script = new Script(ScriptType.INLINE, "painless", scripts, params)
    println(s"script=$script")

    val indexRequest = new IndexRequest(idxName, typeName, esID).source(params)
    val response = transportClient.prepareUpdate(idxName, typeName, esID)
      .setScript(script)
      .setRetryOnConflict(retryNumber)
      .setUpsert(indexRequest)
      .get()
    if (response.status() != RestStatus.CREATED && response.status() != RestStatus.OK) {
      System.err.println("ESForeachWriter occur error!map:" + new ObjectMapper().writeValueAsString(value))
      throw new Exception("run script exception:status:" + response.status().name())
    }
  }


  /**
    * es资源关闭
    * @param errorOrNull
    */
  override def close(errorOrNull: Throwable): Unit = {
    if (this.transportClient != null) {
      //this.transportClient.close()
    }
  }
}
