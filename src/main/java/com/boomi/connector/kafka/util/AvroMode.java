package com.boomi.connector.kafka.util;

public enum AvroMode {
    NO_MESSAGE(0, "No Message"),
    MESSAGE_ONLY(1, "Message Only"),
    MESSAGE_AND_KEY(2, "Message and Key");

    private final int code;
    private final String description;

    private AvroMode(int code, String description) {
        this.code = code;
        this.description = description;
    }

    public int getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }

    public static AvroMode getByCode(int code) {
        for (AvroMode status : values()) {
            if (status.getCode() == code) {
                return status;
            }
        }
        throw new IllegalArgumentException("Invalid code: " + code);
    }
}

