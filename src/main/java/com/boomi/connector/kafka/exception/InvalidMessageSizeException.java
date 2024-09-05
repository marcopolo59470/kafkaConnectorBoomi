package com.boomi.connector.kafka.exception;

/**
 * Custom exception which is thrown when an attempt is made to publish a message to Apache Kafka service which size is
 * higher than the maximum allowed value.
 */
public class InvalidMessageSizeException extends Exception {

    public InvalidMessageSizeException(String message) {
        super(message);
    }


    public InvalidMessageSizeException(String message, Throwable t) {
        super(message, t);
    }
}
