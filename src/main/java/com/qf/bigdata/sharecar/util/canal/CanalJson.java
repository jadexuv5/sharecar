package com.qf.bigdata.sharecar.util.canal;


import java.io.Serializable;

/**
 * Created by finup on 2018/12/4.
 */
public class CanalJson implements Serializable {

    private String tableName;
    private String id;
    private String datas;


    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDatas() {
        return datas;
    }

    public void setDatas(String datas) {
        this.datas = datas;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
