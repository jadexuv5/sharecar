package com.qf.bigdata.sharecar.util.jdbc;

import com.qf.bigdata.sharecar.util.PropertyUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * jdbc工具类
 */
public class JDBCUtil implements Serializable{

    private final static Logger log = LoggerFactory.getLogger(JDBCUtil.class);

    private static String driverName = "com.mysql.jdbc.Driver";

    private static String jdbcConf = "jdbc.properties";

    private static String DEF_TABLE = "test.users";

    public static JDBCCof readJDBC(String confPath){
        JDBCCof conf = null;
        try{

            if(StringUtils.isEmpty(confPath)){
                confPath = jdbcConf;
            }
            Properties pro = PropertyUtil.readProperties(confPath);
            String user = pro.getProperty("user");
            String password = pro.getProperty("password");
            String url = pro.getProperty("url");

            conf = new JDBCCof(url, user, password);
        }catch (Exception e){
            log.error("JDBCUtil.read.err", e);
        }
        return conf;
    }

    public static Map<String,String> jdbc4Ins(String confPath) throws SQLException {
        Map<String, String> result = new HashMap<String, String>();
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            log.error("jdbc.Class.forName.err=", e);
            System.exit(1);
        }

        JDBCCof conf = readJDBC(confPath);
        if (null == conf) {
            log.error("jdbc.read.err");
            throw new SQLException("dbc.read.err");
        }

        String url = conf.getUrl();
        String user = conf.getUser();
        String pass = conf.getPassword();

        //replace "hive" here with the name of the user the queries should run as
        Connection con = DriverManager.getConnection(url, user, pass);
        Statement stmt = con.createStatement();

        // show tables
        String sql = "select * from " + DEF_TABLE;
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
            String db_mysql = res.getString("db_mysql");
            String table_mysql = res.getString("table_mysql");
            String pid = res.getString("pid");
            if (StringUtils.isNotEmpty(db_mysql) && StringUtils.isNotEmpty(table_mysql) && StringUtils.isNotEmpty(pid)) {
                result.put(db_mysql + "." + table_mysql, pid);
            }
        }

        return result;
    }



}
