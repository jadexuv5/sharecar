package com.qf.bigdata.sharecar.enumes;

import java.util.Arrays;
import java.util.List;

public enum VehicleStatusEnum {

    UNUSE("0", "未使用"),
    NORMAL("1", "行驶"),
    MODIFY("2", "修理"),
    SCRAP("9", "报废");


    private String code;
    private String desc;

    private VehicleStatusEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }


    public static List<String> getVehicleStatus(){
        List<String> status = Arrays.asList(
                UNUSE.code,
                NORMAL.code,
                MODIFY.code,
                SCRAP.code
        );
        return status;
    }

    public String getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

}
