package com.qf.bigdata.sharecar.util.canal;

import java.io.Serializable;

/**
 * canal 配置参数
 */
public class CanalConf implements Serializable{

    private Integer canalModel;//9, "集群模式" | 1, "单点模式"
    private String canalZK;//集群zk
    private String canalDestination;//目标数据源
    private Integer canalExceptionStrategy;//异常策略：
    // * 1:retry，重试，重试默认为3次，由retryTimes参数决定，如果重试次数达到阈值，则跳过，并且记录日志。
    // * 2:ignore,直接忽略，不重试，记录日志
    private Integer canalRetryTimes; //重试次数
    private String canalFilter;//表级过滤

    private String canalUser;
    private String canalPass;
    private Integer canalBatchSize;

    private String canalHost;
    private Integer canalPort;


    @Override
    public String toString() {
        return "CanalConf{" +
                "canalModel=" + canalModel +
                ", canalZK='" + canalZK + '\'' +
                ", canalDestination='" + canalDestination + '\'' +
                ", canalExceptionStrategy=" + canalExceptionStrategy +
                ", canalRetryTimes=" + canalRetryTimes +
                ", canalFilter='" + canalFilter + '\'' +
                ", canalUser='" + canalUser + '\'' +
                ", canalPass='" + canalPass + '\'' +
                ", canalBatchSize=" + canalBatchSize +
                ", canalHost='" + canalHost + '\'' +
                ", canalPort=" + canalPort +
                '}';
    }

    public String getCanalHost() {
        return canalHost;
    }

    public void setCanalHost(String canalHost) {
        this.canalHost = canalHost;
    }

    public Integer getCanalPort() {
        return canalPort;
    }

    public void setCanalPort(Integer canalPort) {
        this.canalPort = canalPort;
    }

    public String getCanalDestination() {
        return canalDestination;
    }

    public void setCanalDestination(String canalDestination) {
        this.canalDestination = canalDestination;
    }

    public String getCanalUser() {
        return canalUser;
    }

    public void setCanalUser(String canalUser) {
        this.canalUser = canalUser;
    }

    public String getCanalPass() {
        return canalPass;
    }

    public void setCanalPass(String canalPass) {
        this.canalPass = canalPass;
    }

    public Integer getCanalBatchSize() {
        return canalBatchSize;
    }

    public void setCanalBatchSize(Integer canalBatchSize) {
        this.canalBatchSize = canalBatchSize;
    }

    public String getCanalZK() {
        return canalZK;
    }

    public void setCanalZK(String canalZK) {
        this.canalZK = canalZK;
    }

    public Integer getCanalExceptionStrategy() {
        return canalExceptionStrategy;
    }

    public void setCanalExceptionStrategy(Integer canalExceptionStrategy) {
        this.canalExceptionStrategy = canalExceptionStrategy;
    }

    public Integer getCanalRetryTimes() {
        return canalRetryTimes;
    }

    public void setCanalRetryTimes(Integer canalRetryTimes) {
        this.canalRetryTimes = canalRetryTimes;
    }

    public String getCanalFilter() {
        return canalFilter;
    }

    public void setCanalFilter(String canalFilter) {
        this.canalFilter = canalFilter;
    }

    public Integer getCanalModel() {
        return canalModel;
    }

    public void setCanalModel(Integer canalModel) {
        this.canalModel = canalModel;
    }
}
