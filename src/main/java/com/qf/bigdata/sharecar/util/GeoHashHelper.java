package com.qf.bigdata.sharecar.util;

import ch.hsr.geohash.BoundingBox;
import ch.hsr.geohash.GeoHash;
import ch.hsr.geohash.WGS84Point;
import com.alibaba.fastjson.JSON;
import com.qf.bigdata.sharecar.constant.CommonConstant;
import com.qf.bigdata.sharecar.enumes.GeoDictEnum;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * GeoHash工具类
 */
public class GeoHashHelper implements Serializable {

    private final static Logger log = LoggerFactory.getLogger(GeoHashHelper.class);


    public final static int DEF_PRECISION = 8;

    //经纬度范围
    public final static double LAT_MAX = CommonConstant.LATITUDE_QF_MAX;
    public final static double LAT_MIN = CommonConstant.LATITUDE_QF_MIN;
    public final static double LNG_MAX = CommonConstant.LONGITUDE_QF_MAX;
    public final static double LNG_MIN = CommonConstant.LONGITUDE_QF_MIN;


    /**
     * 经纬度转Base32的geohash字符串
     * @param precision
     * @return
     */
    public static String getGeoHash(String lngStr, String latStr, int precision){
        String geoHashCode = null;
        try{
            if(StringUtils.isNotEmpty(lngStr) && StringUtils.isNotEmpty(latStr)){
                double lat = Double.valueOf(latStr);
                double lng = Double.valueOf(lngStr);

                GeoHash geoHash = GeoHash.withCharacterPrecision(lat, lng, precision);
                geoHashCode = geoHash.toBase32();
            }
        }catch(Exception e){
            log.error("GeoHashHelper.getGeoHash={}", e.getMessage());
        }

        return geoHashCode;
    }

    /**
     * 经纬度转Base32的geohash字符串
     * @param lng
     * @param lat
     * @param precision
     * @return
     */
    public static String getGeoHash(double lng, double lat, int precision){
        String geoHashCode = null;
        try{
            GeoHash geoHash = GeoHash.withCharacterPrecision(lat, lng, precision);
            geoHashCode = geoHash.toBase32();

        }catch(Exception e){
            log.error("GeoHashHelper.getGeoHash={}", e.getMessage());
        }

        return geoHashCode;
    }


    /**
     * 坐标是否在范围内
     * @param lng
     * @param lat
     * @param geoHashCode
     * @return
     */
    public static Boolean isInWithGeoHash(double lng, double lat, String geoHashCode){
        boolean isIn = false;
        try{
            GeoHash geoHash = GeoHash.fromGeohashString(geoHashCode);
            WGS84Point point = new WGS84Point(lat, lng);
            isIn = geoHash.contains(point);
        }catch(Exception e){
            log.error("GeoHashHelper.isInWithGeoHash={}", e.getMessage());
        }
        return isIn;
    }


    /**
     * 距离
     * @param geoHashCode1
     * @param geoHashCode2
     * @return
     */
    public static long distGeoHashCode(String geoHashCode1, String geoHashCode2){
        Long dist = 0l;
        try{
            if(StringUtils.isNotEmpty(geoHashCode1) && StringUtils.isNotEmpty(geoHashCode2)){
                GeoHash geoHash1 = GeoHash.fromGeohashString(geoHashCode1);
                GeoHash geoHash2 = GeoHash.fromGeohashString(geoHashCode2);
                dist = Math.abs(GeoHash.stepsBetween(geoHash1, geoHash2));
            }
        }catch(Exception e){
            log.error("GeoHashHelper.distGeoHashCode={}", e.getMessage());
        }

        return dist;
    }

    public static void main(String[] args) {
        int precision = 8;

        //北京市昌平区回龙观街道住总·旗胜家园(北二区)SOCO公社 "latitude":"40.06615","longitude":"116.36289"
        double lng1 = 116.36289d;
        double lat1 = 40.06615d;
        log.info("GeoHash lng={},lat={},precision={}",lng1, lat1, precision);
        String geoHashCode1 = getGeoHash(lng1, lat1, precision);
        log.info("geoHashCode1={}",geoHashCode1);

        //北京市海淀区西三旗街道枫丹丽舍latitude":"40.06385d","longitude":116.35978d"
        double lng2 = 116.35978d;
        double lat2 = 40.06385d;
        log.info("GeoHash lng2={},lat2={}",lng1, lat1);
        String geoHashCode2 = getGeoHash(lng2, lat2, precision);
        log.info("geoHashCode2={}",geoHashCode2);


        long dist = distGeoHashCode(geoHashCode1, geoHashCode2);
        log.info("dist={}",dist);





    }


}
