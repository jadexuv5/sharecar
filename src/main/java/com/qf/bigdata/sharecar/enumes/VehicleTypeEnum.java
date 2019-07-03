package com.qf.bigdata.sharecar.enumes;

import java.util.Arrays;
import java.util.List;

public enum VehicleTypeEnum {

    BIKE("1", "单车"),
    CAR("2", "汽车");


    private String code;
    private String desc;

    private VehicleTypeEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }


    public static List<String> getVehicleTypes(){
        List<String> actions = Arrays.asList(
                BIKE.code
        );
        return actions;
    }

    public String getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

}
