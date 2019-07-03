package com.qf.bigdata.sharecar.util.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * Created by finup on 2018/11/27.
 */
public class HiveJDBCUtil {

    private final static Logger log = LoggerFactory.getLogger(HiveJDBCUtil.class);

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void jdbc() throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        //replace "hive" here with the name of the user the queries should run as
        Connection con = DriverManager.getConnection("jdbc:hive2://127.0.0.1:10000/ods", "finup", "");
        Statement stmt = con.createStatement();



        // show tables
        String sql = "show tables in ods";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1));
        }

        //refresh table
        //stmt.executeUpdate("refresh table  employee");

        // show tables
        String sql2 = "select ct,from_unixtime(cast(ct/1000 as bigint),'yyyyMMddHH') as t1 from ods.release_sessions limit 10";
        System.out.println("Running: " + sql2);
        ResultSet res2 = stmt.executeQuery(sql2);

//        ResultSetMetaData rsmd = res2.getMetaData();
//        while(rsmd.){
//
//        }

        int idx = 0;
        while (res2.next()) {
            System.out.println(String.format("row[%d]=============", idx++));
            Long ct = res2.getLong("ct");
            String t1 = res2.getString("t1");
            System.out.println(String.format("ct=%d,t1=%s", ct, t1));
        }

//        int idx = 0;
//        while (res2.next()) {
//            System.out.println(String.format("row[%d]=============", idx++));
//            String id = res2.getString("id");
//            String name = res2.getString("name");
//            Integer age = res2.getInt("age");
//            String birthday = res2.getString("birthday");
//            String nation = res2.getString("nation");
//            Double salary = res2.getDouble("salary");
//
//            //array
//            String loves = res2.getString("loves");
//            //String lovesJson = JSON.toJSONString(loves);
//
//            String info = res2.getString("info");
//            String familyJson  = res2.getString("family");
//            Map<String,String>  family = JSON.parseObject(familyJson,Map.class);
//
//            System.out.println(String.format("id=%s,name=%s,age=%d,birthday=%s", id, name, age, birthday));
//            System.out.println(String.format("nation=%s,salary=%f", nation, salary));
//            System.out.println(String.format("loves=%s", loves));
//            System.out.println(String.format("info=%s", info));
//            System.out.println(String.format("familyJson=%s", familyJson));
//            System.out.println("family==>" + family);
//        }
    }

}
