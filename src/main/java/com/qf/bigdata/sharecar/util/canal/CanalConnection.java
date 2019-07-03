package com.qf.bigdata.sharecar.util.canal;

import com.alibaba.otter.canal.client.CanalConnector;

import java.io.Serializable;

/**
 * canal连接
 */
public class CanalConnection implements Serializable {

    private CanalConf conf;
    private CanalConnector connector;

    public CanalConnection(CanalConf conf,CanalConnector connector){
        this.conf = conf;
        this.connector = connector;
    }



    public CanalConf getConf() {
        return conf;
    }

    public void setConf(CanalConf conf) {
        this.conf = conf;
    }

    public CanalConnector getConnector() {
        return connector;
    }

    public void setConnector(CanalConnector connector) {
        this.connector = connector;
    }
}
