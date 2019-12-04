package com.qf.bigdata.sharecar.util.jdbc;

import java.io.Serializable;

/**
 * jdbc配置参数
 */
public class JDBCCof implements Serializable {

    private String url;
    private String user;
    private String password;

    public JDBCCof(String url,String user,String password){
        this.url = url;
        this.user = user;
        this.password = password;
    }


    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}