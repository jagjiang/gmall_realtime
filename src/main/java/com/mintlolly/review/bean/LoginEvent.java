package com.mintlolly.review.bean;

import lombok.Data;

/**
 * Created on 2022/5/1
 *
 * @author jiangbo
 * Description:
 */
@Data
public class LoginEvent {
    private String userId;
    private String ipAddress;
    private String eventType;
    private Long timestamp;

    public LoginEvent(String userId, String ipAddress, String eventType, Long timestamp) {
        this.userId = userId;
        this.ipAddress = ipAddress;
        this.eventType = eventType;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "LoginEvent{" +
                "userId='" + userId + '\'' +
                ", ipAddress='" + ipAddress + '\'' +
                ", eventType='" + eventType + '\'' +
                ", timestamp='" + timestamp + '\'' +
                '}';
    }
}
