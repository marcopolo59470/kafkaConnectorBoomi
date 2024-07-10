
package com.boomi.connector.kafka.util;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;

import java.util.Collection;
import java.util.logging.Level;

public class ResultUtil {

    private ResultUtil() {
    }

    public static void addApplicationError(ObjectData data, OperationResponse response, String code, String message,
            Throwable t) {
        data.getLogger().log(Level.WARNING, message, t);
        response.addResult(data, OperationStatus.APPLICATION_ERROR, code, message, null);
    }

    public static void addFailure(ObjectData data, OperationResponse response, String code, Throwable t) {
        String message = ConnectorException.getStatusMessage(t);
        data.getLogger().log(Level.SEVERE, message, t);
        response.addErrorResult(data, OperationStatus.FAILURE, code, message, t);
    }

    public static void addSuccesses(Collection<ObjectData> datas, OperationResponse response) {
        for (ObjectData data : datas) {
            response.addResult(data, OperationStatus.SUCCESS, Constants.CODE_SUCCESS, Constants.CODE_SUCCESS, null);
        }
    }

    public static void addApplicationErrors(Collection<ObjectData> datas, OperationResponse response, String code,
            String message, Throwable t) {
        for (ObjectData data : datas) {
            addApplicationError(data, response, code, message, t);
        }
    }
}
