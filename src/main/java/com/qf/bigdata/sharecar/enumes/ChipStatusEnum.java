package com.qf.bigdata.sharecar.enumes;

import java.util.Arrays;
import java.util.List;

public enum ChipStatusEnum {

    UNUSE("0", "未使用"),
    NORMAL("1", "行驶"),
    SCRAP("9", "报废");


    private String code;
    private String desc;

    private ChipStatusEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }


    public static List<String> getChipStatuss(){
        List<String> actions = Arrays.asList(
                UNUSE.code,
                NORMAL.code,
                SCRAP.code
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
