package com.qf.bigdata.sharecar.domain;

import java.io.Serializable;

/**
 * 芯片
 */
public class Chip implements Serializable {

    private String chipCode;//芯片编码
    private String chipStatus; //芯片状态: 0未使用|1使用|9损坏
    private String vehicleCode; //车辆编码
    private String ctTime;

    public String getChipCode() {
        return chipCode;
    }

    public void setChipCode(String chipCode) {
        this.chipCode = chipCode;
    }

    public String getChipStatus() {
        return chipStatus;
    }

    public void setChipStatus(String chipStatus) {
        this.chipStatus = chipStatus;
    }

    public String getVehicleCode() {
        return vehicleCode;
    }

    public void setVehicleCode(String vehicleCode) {
        this.vehicleCode = vehicleCode;
    }

    public String getCtTime() {
        return ctTime;
    }

    public void setCtTime(String ctTime) {
        this.ctTime = ctTime;
    }
}
