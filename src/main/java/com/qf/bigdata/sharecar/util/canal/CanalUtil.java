package com.qf.bigdata.sharecar.util.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.qf.bigdata.sharecar.enumes.CanalModelEnum;
import com.qf.bigdata.sharecar.util.json.JsonHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by finup on 2018/12/1.
 */
public class CanalUtil implements Serializable {

    private final static Logger log = LoggerFactory.getLogger(CanalService.class);

    /**
     * 配置文件列表
     * @return
     * @throws Exception
     */
    public static List<CanalConf> createCanalConf() throws Exception{
        List<CanalConf> confs = (List<CanalConf>) JsonHelper.parseJsonArr("canal/canal.json", CanalConf.class);
        return confs;
    }


    /**
     * canal客户端连接
     * @return
     * @throws Exception
     */
    public static List<CanalConnection> createConnections() throws Exception{
        List<CanalConnection> connectors = new ArrayList<CanalConnection>();
        try {
            List<CanalConf> confs = createCanalConf();
            if(null != confs){
                for(CanalConf conf:confs){
                    CanalConnector connector = createConnector4Cluster(conf);
                    CanalConnection connection = new CanalConnection(conf, connector);
                    connectors.add(connection);
                }
            }
        }catch (Exception e){
            log.error("CanalUtil.createConnectors.error:", e);
        }
        return connectors;
    }

    /**
     * canal客户端连接
     * @param canalConf
     * @return
     * @throws Exception
     */
    public static CanalConnector createConnector(CanalConf canalConf) throws Exception{
        // 创建链接
        CanalConnector connector = null;
        Integer model = canalConf.getCanalModel();
        String username = canalConf.getCanalUser();
        String password = canalConf.getCanalPass();

        if(CanalModelEnum.CONSUMER_CLUSTER.getType() == model){
            String canalZK = canalConf.getCanalZK();
            String destination = canalConf.getCanalDestination();//实例
            connector = CanalConnectors.newClusterConnector(canalZK,destination, username, password);

        }else if(CanalModelEnum.CONSUMER_SINGLE.getType() == model){
            String host = canalConf.getCanalHost();
            Integer port = canalConf.getCanalPort();
            String destination = canalConf.getCanalDestination();//实例

            SocketAddress address = new InetSocketAddress(host, port);
            connector = CanalConnectors.newSingleConnector(address,destination, username, password);
        }

        return connector;
    }


    /**
     * canal客户端连接
     * @param canalConf
     * @return
     * @throws Exception
     */
    private static CanalConnector createConnector4Sigle(CanalConf canalConf) throws Exception{
        // 创建链接
        String host = canalConf.getCanalHost();
        Integer port = canalConf.getCanalPort();
        String destination = canalConf.getCanalDestination();//实例

        SocketAddress address = new InetSocketAddress(host, port);
        CanalConnector connector = CanalConnectors.newSingleConnector(address,destination, "", "");
        return connector;
    }

    /**
     * 集群模式
     * @param canalConf
     * @return
     * @throws Exception
     */
    private static CanalConnector createConnector4Cluster(CanalConf canalConf) throws Exception{
        // 创建链接
        String canalZK = canalConf.getCanalZK();
        String destination = canalConf.getCanalDestination();//实例
        log.info(canalZK);

        CanalConnector connector = CanalConnectors.newClusterConnector(canalZK, destination, "", "");
        return connector;
    }


}
