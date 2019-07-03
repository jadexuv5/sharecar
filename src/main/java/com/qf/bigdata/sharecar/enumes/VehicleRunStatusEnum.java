package com.qf.bigdata.sharecar.enumes;

import java.util.Arrays;
import java.util.List;

public enum VehicleRunStatusEnum {

    BEGIN("0", "开始"),
    NORMAL("1", "中间"),
    END("9", "结束");


    private String code;
    private String desc;

    private VehicleRunStatusEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }


    public static List<String> getVehicleStatus(){
        List<String> status = Arrays.asList(
                BEGIN.code,
                NORMAL.code,
                END.code
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
