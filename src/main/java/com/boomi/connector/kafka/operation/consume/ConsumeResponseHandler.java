
package com.boomi.connector.kafka.operation.consume;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.kafka.util.Constants;
import com.boomi.util.CollectionUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

/**
 * Handler in charge of adding the {@link Payload}s and errors to the wrapped {@link OperationResponse}
 */
class ConsumeResponseHandler {

    private final List<ObjectData> _inputs = new ArrayList<>();
    private final OperationResponse _response;
    private boolean _hasError;
    private boolean _hasSuccess;

    ConsumeResponseHandler(UpdateRequest request, OperationResponse response) {
        CollectionUtil.addAll(request, _inputs);
        _response = response;
    }

    /**
     * Adds the given payload as a Partial Success
     *
     * @param payload
     *         the payload to add
     */
    void addSuccess(Payload payload) {
        ResponseUtil.addPartialSuccesses(_response, _inputs, Constants.CODE_SUCCESS, payload);
        _hasSuccess = true;
    }

    void addApplicationError(Payload payload, String message, String code) {
        _response.addPartialResult(_inputs, OperationStatus.APPLICATION_ERROR, code, message, payload);
    }

    /**
     * adds the given {@link Throwable} as a Failure
     *
     * @param t
     *         the throwable to add
     */
    void addFailure(Throwable t, String code) {
        String message = ConnectorException.getStatusMessage(t);
        _response.getLogger().log(Level.WARNING, message, t);
        _response.addPartialResult(_inputs, OperationStatus.FAILURE, code, message, null);
        _hasError = true;
    }

    void log(Level level, String message, Throwable t) {
        _response.getLogger().log(level, message, t);
    }

    /**
     * Closes the {@link OperationResponse} results.
     * This method must be called after adding all the results.
     */
    void finish() {
        boolean hasAddedResults = _hasSuccess || _hasError;
        if (hasAddedResults) {
            _response.finishPartialResult(_inputs);
        } else {
            ResponseUtil.addEmptySuccesses(_response, _inputs, Constants.CODE_SUCCESS);
        }
    }
}
