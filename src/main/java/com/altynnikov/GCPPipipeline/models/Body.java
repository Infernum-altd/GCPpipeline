package com.altynnikov.GCPPipipeline.models;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Body {
    private Message message;

    @Data
    @NoArgsConstructor
    public class Message {

        private String messageId;
        private String publishTime;
        private String data;

    }
}
