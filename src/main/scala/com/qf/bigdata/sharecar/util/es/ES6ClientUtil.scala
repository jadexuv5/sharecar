package com.qf.bigdata.sharecar.util.es;


import java.net.InetAddress

import com.qf.bigdata.sharecar.util.es.ESConfigUtil.ESConfig
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.JavaConverters._

/**
  * es
  */
object ES6ClientUtil {

  //val logger :Logger = LoggerFactory.getLogger(ES6ClientUtil.getClass)


  //es连接
  def buildTransportClient(esConfigPath: String): TransportClient = {
    if (null == esConfigPath) {
      throw new RuntimeException("esConfigPath is null!")
    }
    val esConfig: ESConfig = ESConfigUtil.getConfig(esConfigPath)
    val transAddrs :mutable.Buffer[TransportAddress] = esConfig.transportAddresses.asScala

    //es 参数
    val settingBuilder :Settings.Builder = Settings.builder()
    for((key, value) <- esConfig.config.asScala){
      settingBuilder.put(key, value)
    }

    //es 集群节点链接信息
    val transportClient = new PreBuiltTransportClient(settingBuilder.build()).addTransportAddresses(transAddrs:_*)

    var info = ""
    if (transportClient.connectedNodes.isEmpty) {
      info = "Elasticsearch client is not connected to any Elasticsearch nodes!"
      //logger.error(info)
      throw new RuntimeException(info)
    }else {
      info = "Created Elasticsearch TransportClient with connected nodes {}"+ transportClient.connectedNodes
      //logger.info(info)
    }
    transportClient
  }


  //es连接
  def buildTransportClientTest(): TransportClient = {


    //es 参数
    val settings :Settings = Settings.builder()
        .put("cluster.name", "qf-es")
        .put("client.transport.sniff", true)
        .build();


    //es 集群节点链接信息
    val transportClient = new PreBuiltTransportClient(settings)
      .addTransportAddress(new TransportAddress(InetAddress.getByName("10.0.88.242"), 9300))
      .addTransportAddress(new TransportAddress(InetAddress.getByName("10.0.88.243"), 9300))
      .addTransportAddress(new TransportAddress(InetAddress.getByName("10.0.88.244"), 9300));

    var info = ""
    if (transportClient.connectedNodes.isEmpty) {
      info = "Elasticsearch client is not connected to any Elasticsearch nodes!"
      //logger.error(info)
      throw new RuntimeException(info)
    }else {
      info = "Created Elasticsearch TransportClient with connected nodes {}"+ transportClient.connectedNodes
      //logger.info(info)
    }
    transportClient
  }


  def main(args: Array[String]): Unit = {

    System.setProperty("es.set.netty.runtime.available.processors", "false")

    val esConfigPath = "es/es-config.json"
    val transportClient = ES6ClientUtil.buildTransportClient(esConfigPath)

    //val transportClient = ES6ClientUtil.buildTransportClient2()

    if (transportClient.connectedNodes.isEmpty) {
      print("transportClient.connectedNodes.isEmpty")
    }else {
      print("es is ok")
    }

  }

}
