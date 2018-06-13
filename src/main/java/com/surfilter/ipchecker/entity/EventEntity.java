package com.surfilter.ipchecker.entity;

import java.io.Serializable;

public class EventEntity implements Serializable {

    private String ip; //ip地址信息
    private String eventType; //事件类型
    private String ipOperator;//运营商

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getIpOperator() {
        return ipOperator;
    }

    public void setIpOperator(String ipOperator) {
        this.ipOperator = ipOperator;
    }

    @Override
    public String toString() {
        return "EventEntity{" +
                "ip='" + ip + '\'' +
                ", eventType='" + eventType + '\'' +
                ", ipOperator='" + ipOperator + '\'' +
                '}';
    }
}
