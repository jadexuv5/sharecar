package com.qf.bigdata.sharecar.domain;

import java.io.Serializable;
import java.util.Date;

/**
 * 用户
 */
public class User implements Serializable {

    private String userCode;//用户标识(手机)
    private String userCards;//用户身份证(实名认证)
    private String userPass;//用户密码
    private Date ctTime;//创建时间

    public String getUserCode() {
        return userCode;
    }

    public void setUserCode(String userCode) {
        this.userCode = userCode;
    }

    public String getUserPass() {
        return userPass;
    }

    public void setUserPass(String userPass) {
        this.userPass = userPass;
    }

    public String getUserCards() {
        return userCards;
    }

    public void setUserCards(String userCards) {
        this.userCards = userCards;
    }

    public Date getCtTime() {
        return ctTime;
    }

    public void setCtTime(Date ctTime) {
        this.ctTime = ctTime;
    }
}
