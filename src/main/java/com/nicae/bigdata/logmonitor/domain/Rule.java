package com.nicae.bigdata.logmonitor.domain;

/**
 * Created by Rainbow on 2016/12/8.
 */
public class Rule {

    private int id;//规则编号
    private String name;//规则名称
    private String keyword;//规则过滤的关键字
    private int isValid;//规则是否可用
    private int appId;//规则所属的应用

    public int getAppId() {
        return appId;
    }

    public void setAppId(int appId) {
        this.appId = appId;
    }

    public String getKeyword() {
        return keyword;
    }

    public void setKeyword(String keyword) {
        this.keyword = keyword;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getIsValid() {
        return isValid;
    }

    public void setIsValid(int isValid) {
        this.isValid = isValid;
    }
}
