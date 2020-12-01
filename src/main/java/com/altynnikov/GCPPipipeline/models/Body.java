package com.altynnikov.GCPPipipeline.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

@Data
@NoArgsConstructor
public class Body {
    private Message message;

    @Data
    @AllArgsConstructor
    public static class Message {

        private String messageId;
        private String publishTime;
        private String data;
    }

    public boolean isMessageValid() {
        return message != null && !StringUtils.isEmpty(message.getData());
    }
}
