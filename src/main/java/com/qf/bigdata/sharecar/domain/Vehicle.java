package com.qf.bigdata.sharecar.domain;

import java.io.Serializable;
import java.util.Date;

/**
 * 车辆
 */
public class Vehicle implements Serializable {

    private String vehicleCode;//车辆编码
    private String chipCode;//芯片编码
    private String vehicleType;//运输工具类型
    private String vehicleStatus; //状态：0未使用 1行驶 2修理 3报废
    private Date ctTime;//当前时间

    public String getVehicleCode() {
        return vehicleCode;
    }

    public void setVehicleCode(String vehicleCode) {
        this.vehicleCode = vehicleCode;
    }

    public String getVehicleType() {
        return vehicleType;
    }

    public void setVehicleType(String vehicleType) {
        this.vehicleType = vehicleType;
    }

    public String getVehicleStatus() {
        return vehicleStatus;
    }

    public void setVehicleStatus(String vehicleStatus) {
        this.vehicleStatus = vehicleStatus;
    }

    public Date getCtTime() {
        return ctTime;
    }

    public void setCtTime(Date ctTime) {
        this.ctTime = ctTime;
    }
}
