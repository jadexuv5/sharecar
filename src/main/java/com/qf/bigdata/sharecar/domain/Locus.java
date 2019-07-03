package com.qf.bigdata.sharecar.domain;

import com.qf.bigdata.sharecar.constant.CommonConstant;
import com.qf.bigdata.sharecar.dvo.GisDO;
import com.qf.bigdata.sharecar.dvo.UovDO;
import com.qf.bigdata.sharecar.enumes.NetWorkSignalTypeEnum;
import com.qf.bigdata.sharecar.enumes.VehicleRunStatusEnum;
import com.qf.bigdata.sharecar.util.AmapGisUtil;
import com.qf.bigdata.sharecar.util.CommonUtil;
import com.qf.bigdata.sharecar.util.GeoHashHelper;
import com.qf.bigdata.sharecar.util.json.JsonUtil;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.io.Serializable;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * 轨迹行程
 */
public class Locus implements Serializable {

    private String userCode;//用户编码
    private String orderCode;//行程单号
    private String vehicleCode;//车辆编码
    private String longitude;//经度
    private String latitude;//纬度
    private String geoHash; //geohash代码
    private String adcode;//城市编码 110108
    private String province;//省 北京市
    private String district;//地区 海淀区
    private String address;//formatted_address 地址 北京市海淀区西三旗街道青岛啤酒五星快乐驿站金隅·翡丽
    private String status;//轨迹状态：0 开始 1 中间 9结束
    private String gSignal; //网络信号强度
    private Long ctTime;//当前时间


    /**
     * 车辆行驶定位记录
     * @param begin
     * @param end
     * @return
     */
    public static List<Locus> createTemp(Locus begin, Locus end){
        List<Locus> locuss = new ArrayList<Locus>();
        ChronoUnit chronoUnit = ChronoUnit.MINUTES;
        String dateFormatter = CommonConstant.FORMATTER_YYYYMMDDHHMMDD;
        if(null != begin && null != end){
            //时间
            Date beginTime = new Date(begin.getCtTime());
            String btStr = CommonUtil.formatDate(beginTime,dateFormatter);

            Date endTime = new Date(end.getCtTime());
            String etStr = CommonUtil.formatDate(endTime,dateFormatter);

            int diff = CommonUtil.calculateTimeDifferenceByChronoUnit(dateFormatter, btStr, etStr, chronoUnit).intValue();
            for(int i=1;i<diff;i++){
                Date ctTime = CommonUtil.getTime(beginTime, i, Calendar.MINUTE);

                Locus locus = new Locus();
                String userCode = begin.getUserCode();
                String orderCode = begin.getOrderCode();
                String vehicleCode = begin.getVehicleCode();

                //用户编码
                locus.setUserCode(userCode);
                //订单编码
                locus.setOrderCode(orderCode);
                //车辆编码
                locus.setVehicleCode(vehicleCode);

                //经度|纬度
                List<GisDO> giss = AmapGisUtil.initDatas();
                GisDO gisDO = CommonUtil.getRandomElementRange(giss);
                String lng = gisDO.getLongitude();
                locus.setLongitude(lng);
                String lat = gisDO.getLatitude();
                locus.setLatitude(lat);
                locus.setAdcode(gisDO.getAdcode());
                locus.setProvince(gisDO.getProvince());
                locus.setDistrict(gisDO.getDistrict());
                locus.setAddress(gisDO.getAddress());

                //geohash
                String geoHashCode = GeoHashHelper.getGeoHash(lng, lat, GeoHashHelper.DEF_PRECISION);
                locus.setGeoHash(geoHashCode);

                //状态
                locus.setStatus(VehicleRunStatusEnum.NORMAL.getCode());

                //网络信号
                List<String> gSignals = NetWorkSignalTypeEnum.getNetWorkSignalTypes();
                String gSignal = CommonUtil.getRandomElementRange(gSignals);
                locus.setgSignal(gSignal);

                //时间
                locus.setCtTime(ctTime.getTime());

                locuss.add(locus);
            }
        }
        return locuss;
    }


    /**
     * 开始结束定位
     * @param params
     * @param isBegin
     * @return
     */
    public static Locus createBeginEndTemp(Map<String,String> params,String ctTime, boolean isBegin){
        Locus locus = new Locus();

        //用户编码
        String userCode = params.getOrDefault(CommonConstant.KEY_USER_CODE, "");
        locus.setUserCode(userCode);

        //订单编码
        String orderCode = params.getOrDefault(CommonConstant.KEY_ORDER_CODE, "");
        locus.setOrderCode(orderCode);

        //车辆编码
        String vehicleCode = params.getOrDefault(CommonConstant.KEY_VEHICLE_CODE, "");
        locus.setVehicleCode(vehicleCode);

        //经度|纬度
        List<GisDO> giss = AmapGisUtil.initDatas();
        GisDO gisDO = CommonUtil.getRandomElementRange(giss);
        String lng = gisDO.getLongitude();
        locus.setLongitude(lng);
        String lat = gisDO.getLatitude();
        locus.setLatitude(lat);
        locus.setAdcode(gisDO.getAdcode());
        locus.setProvince(gisDO.getProvince());
        locus.setDistrict(gisDO.getDistrict());
        locus.setAddress(gisDO.getAddress());

        //geohash
        String geoHashCode = GeoHashHelper.getGeoHash(lng, lat, GeoHashHelper.DEF_PRECISION);
        locus.setGeoHash(geoHashCode);

        //状态
        if(isBegin){
            locus.setStatus(VehicleRunStatusEnum.BEGIN.getCode());
        }else{
            locus.setStatus(VehicleRunStatusEnum.END.getCode());
        }

        //网络信号
        List<String> gSignals = NetWorkSignalTypeEnum.getNetWorkSignalTypes();
        String gSignal = CommonUtil.getRandomElementRange(gSignals);
        locus.setgSignal(gSignal);

        //时间
        Date ctDate = CommonUtil.parseText(ctTime,CommonConstant.FORMATTER_YYYYMMDDHHMMDD);
        locus.setCtTime(ctDate.getTime());

        return locus;
    }

    public static Map<String,String> createParams(String userCode,String orderCode,String vehicleCode){
        Map<String,String> params = new HashMap<String,String>();
        params.put(CommonConstant.KEY_USER_CODE,userCode);
        params.put(CommonConstant.KEY_ORDER_CODE,orderCode);
        params.put(CommonConstant.KEY_VEHICLE_CODE,vehicleCode);
        return params;
    }

    /**
     * 单人轨迹
     * @return
     */
    public static List<Locus> createSingleAlls(String userCode,String orderCode,String vehicleCode,String beginTime,String endTime ){
        List<Locus> alls = new ArrayList<>();

        Map<String,String> params = createParams(userCode, orderCode, vehicleCode);
        Locus beginLocus = createBeginEndTemp(params,beginTime,true);
        Locus endLocus = createBeginEndTemp(params,endTime,false);
        List<Locus> locuss = createTemp(beginLocus, endLocus);

        alls.add(beginLocus);
        alls.addAll(locuss);
        alls.add(endLocus);
        return alls;
    }

    public static Map<String,String> createDatas(List<UovDO> uovs,String beginTime,String endTime) throws  Exception{
        Map<String,String> datas = new HashMap<String,String>();
        List<String> dataJsons = new ArrayList<String>();

        for(UovDO uov : uovs){
            String userCode = uov.getUserCode();
            String orderCode = uov.getOrderCode();
            String vehicleCode = uov.getVehicleCode();

            List<Locus> alls = createSingleAlls(userCode, orderCode,vehicleCode, beginTime,endTime);
            for(Locus locus : alls){
                String json = JsonUtil.object2json4DefDateFormat(locus);
                String locusAt = CommonUtil.formatDate(new Date(locus.getCtTime()),CommonConstant.FORMATTER_YYYYMMDDHHMMDD);

                //String random = CommonUtil.getRandom4RK(4);
                String bysKey = locus.getOrderCode()  +  locusAt;

                //hashMD5
                String key = MD5Hash.getMD5AsHex(bysKey.getBytes()).substring(0, 8) + bysKey;
                String line  = "bysKey=" + bysKey +",code="+locus.getOrderCode()+ ",key="+ key +",json["+json+"]";
                dataJsons.add(line);
                datas.put(key, json);
            }
        }
        //FileIOUtil.writeFileLines(logPath, dataJsons, true);
        return datas;
    }

    public String getUserCode() {
        return userCode;
    }

    public void setUserCode(String userCode) {
        this.userCode = userCode;
    }

    public String getOrderCode() {
        return orderCode;
    }

    public void setOrderCode(String orderCode) {
        this.orderCode = orderCode;
    }

    public String getVehicleCode() {
        return vehicleCode;
    }

    public void setVehicleCode(String vehicleCode) {
        this.vehicleCode = vehicleCode;
    }

    public String getLongitude() {
        return longitude;
    }

    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }

    public String getLatitude() {
        return latitude;
    }

    public void setLatitude(String latitude) {
        this.latitude = latitude;
    }

    public String getAdcode() {
        return adcode;
    }

    public void setAdcode(String adcode) {
        this.adcode = adcode;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getDistrict() {
        return district;
    }

    public void setDistrict(String district) {
        this.district = district;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getgSignal() {
        return gSignal;
    }

    public void setgSignal(String gSignal) {
        this.gSignal = gSignal;
    }

    public Long getCtTime() {
        return ctTime;
    }

    public void setCtTime(Long ctTime) {
        this.ctTime = ctTime;
    }

    public String getGeoHash() {
        return geoHash;
    }

    public void setGeoHash(String geoHash) {
        this.geoHash = geoHash;
    }
}
