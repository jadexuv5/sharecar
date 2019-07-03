package com.qf.bigdata.sharecar.util.es;

import java.net.{InetAddress, InetSocketAddress}

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ArrayNode
import org.elasticsearch.common.transport.TransportAddress
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by finup on 2018/12/20.
  */
object ESConfigUtil {

  val logger :Logger = LoggerFactory.getLogger(ESConfigUtil.getClass)

  case class ESConfig(var config: java.util.HashMap[String, String], var transportAddresses: java.util.ArrayList[TransportAddress])

  var esConfig: ESConfig = null

  /**
    * es集群配置信息
    * @param configPath
    * @return
    */
  def getConfig(configPath: String): ESConfig = {
    try{
      val configStream = this.getClass.getClassLoader.getResourceAsStream(configPath)
      if (null == esConfig) {
        val mapper = new ObjectMapper()
        val configJsonObject = mapper.readTree(configStream)

        val configJsonNode = configJsonObject.get("config")
        val config = {
          val configJsonMap = new java.util.HashMap[String, String]
          val it = configJsonNode.fieldNames()
          while (it.hasNext) {
            val key = it.next()
            configJsonMap.put(key, configJsonNode.get(key).asText())
          }
          configJsonMap
        }

        val addressJsonNode = configJsonObject.get("address")
        val addressJsonArray = classOf[ArrayNode].cast(addressJsonNode)
        val transportAddresses = {
          val transportAddresses = new java.util.ArrayList[TransportAddress]
          val it = addressJsonArray.iterator()
          while (it.hasNext) {
            val detailJsonNode: JsonNode = it.next()
            val ip = detailJsonNode.get("ip").asText()
            val port = detailJsonNode.get("port").asInt()
            transportAddresses.add(new TransportAddress(InetAddress.getByName(ip), port))
          }
          transportAddresses
        }

        esConfig = new ESConfig(config, transportAddresses)
      }
    }catch {
      case ex:Exception => logger.error("ESConfigUtil.getConfig.error=>", ex)
    }

    esConfig
  }





}
