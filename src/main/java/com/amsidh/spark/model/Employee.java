package com.amsidh.spark.model;

public class Employee {
    private String name;
    private String emailId;
    private String extraData;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEmailId() {
        return emailId;
    }

    public void setEmailId(String emailId) {
        this.emailId = emailId;
    }

    public String getExtraData() {
        return extraData;
    }

    public void setExtraData(String extraData) {
        this.extraData = extraData;
    }

    @Override
    public String toString() {
        return "Employee{" +
                "name='" + name + '\'' +
                ", emailId='" + emailId + '\'' +
                ", extraData='" + extraData + '\'' +
                '}';
    }
}
