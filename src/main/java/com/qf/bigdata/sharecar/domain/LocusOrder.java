package com.qf.bigdata.sharecar.domain;

import java.io.Serializable;
import java.util.Date;

/**
 * 行驶订单
 */
public class LocusOrder implements Serializable {

    private String orderCode;//行程单号
    private String userCode;//用户编码
    private String vehicleCode;//车辆编码
    private String longitude;//位置经度
    private String latitude;//位置纬度
    private String adcode;//城市编码 110108
    private String province;//省 北京市
    private String district;//地区 海淀区
    private String addr;//formatted_address 地址 北京市海淀区西三旗街道青岛啤酒五星快乐驿站金隅·翡丽
    private String status; //开始1|9结束
    private Date ctTime;//订单创建时间


    public String getOrderCode() {
        return orderCode;
    }

    public void setOrderCode(String orderCode) {
        this.orderCode = orderCode;
    }

    public String getUserCode() {
        return userCode;
    }

    public void setUserCode(String userCode) {
        this.userCode = userCode;
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

    public String getAddr() {
        return addr;
    }

    public void setAddr(String addr) {
        this.addr = addr;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Date getCtTime() {
        return ctTime;
    }

    public void setCtTime(Date ctTime) {
        this.ctTime = ctTime;
    }
}
