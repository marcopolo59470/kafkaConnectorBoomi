package com.boomi.connector.kafka.util;

import java.util.Objects;

public enum AvroMode {
    NO_MESSAGE("0", "No Message"),
    MESSAGE_ONLY("1", "Message Only"),
    MESSAGE_AND_KEY("2", "Message and Key");

    private final String code;
    private final String description;

    AvroMode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    public String getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }

    public static AvroMode getByCode(String code) {
        for (AvroMode status : values()) {
            if (Objects.equals(status.getCode(), code)) {
                return status;
            }
        }
        throw new IllegalArgumentException("Invalid code: " + code);
    }
}

