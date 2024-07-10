
package com.boomi.connector.kafka.util;

import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.concurrent.ExecutionException;

/**
 * Helper class for getting the Error Code associated to {@link org.apache.kafka.common.KafkaException} family
 */
public class ErrorCodeHelper {

    private ErrorCodeHelper() {
    }

    /**
     * Extract the cause of the given exception and return the appropriate error code
     *
     * @param e
     *         the exception to extract the cause
     * @return the error code
     */
    public static String getErrorCode(ExecutionException e) {
        Throwable rootError = e.getCause();

        if (rootError instanceof TimeoutException) {
            return Constants.CODE_TIMEOUT;
        }

        if (rootError instanceof RecordTooLargeException) {
            return Constants.CODE_INVALID_SIZE;
        }

        if (rootError instanceof UnknownTopicOrPartitionException) {
            return Constants.CODE_UNKNOWN_TOPIC;
        }

        return Constants.CODE_ERROR;
    }
}
