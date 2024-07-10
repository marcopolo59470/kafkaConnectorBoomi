
package com.boomi.connector.kafka.operation.produce;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.kafka.exception.InvalidMessageSizeException;
import com.boomi.connector.kafka.operation.KafkaOperationConnection;
import com.boomi.connector.kafka.util.Constants;
import com.boomi.connector.kafka.util.ErrorCodeHelper;
import com.boomi.connector.kafka.util.ResultUtil;
import com.boomi.connector.kafka.util.TopicNameUtil;
import com.boomi.connector.util.BaseUpdateOperation;
import com.boomi.util.IOUtil;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Execute Operation for publishing messages to Apache Kafka.
 * Sends messages to the Topic specified as Object Type.
 */
public class ProduceOperation extends BaseUpdateOperation {

    private static final String ERROR_MESSAGE_TIMEOUT = "timeout error connecting to the service";
    private final String _topic;
    private final boolean _isDynamicTopic;

    public ProduceOperation(KafkaOperationConnection connection) {
        super(connection);
        _topic = TopicNameUtil.getTopic(connection.getObjectTypeId(), connection.getContext());
        _isDynamicTopic = Constants.DYNAMIC_TOPIC_ID.equals(connection.getObjectTypeId());
    }

    @Override
    public KafkaOperationConnection getConnection() {
        return (KafkaOperationConnection) super.getConnection();
    }

    /**
     * Creates a producer to send messages to Apache Kafka service. Then process a collection of type ObjectData which
     * are the messages to publish. If an exception is thrown instantiating the producer or publishing the messages, it
     * stops the process and the result is a failure.
     *
     * @param updateRequest Collection of type ObjectData which are the messages to send to Apache Kafka service.
     * @param response      Where the results are added.
     */
    @Override
    protected void executeUpdate(UpdateRequest updateRequest, OperationResponse response) {
        BoomiProducer producer = null;

        try {
            producer = getConnection().createProducer();

            for (ObjectData data : updateRequest) {
                process(producer, data, response);
            }
        } catch (InterruptedException e) {
            // mark pending documents as Failure
            ResponseUtil.addExceptionFailures(response, updateRequest, e);
            // signal this thread as interrupted
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            // mark pending documents as Failure
            ResponseUtil.addExceptionFailures(response, updateRequest, e);
        } finally {
            IOUtil.closeQuietly(producer);
        }
    }

    /**
     * Sends the given ObjectData as a message to the Apache Kafka service using the given producer and then add the
     * result in the given response.
     *
     * @param producer To publish messages to Apache Kafka service.
     * @param data     The message to send to Apache Kafka service.
     * @param response Where the result is added.
     * @throws ConnectorException If a Timeout error arises.
     */
    private void process(BoomiProducer producer, ObjectData data, OperationResponse response)
            throws InterruptedException {
        ProduceMessage message = null;
        try {
            message = new ProduceMessage(data, _topic, producer.getMaxRequestSize());
            producer.sendMessage(message.toRecord());
            ResponseUtil.addEmptySuccess(response, data, Constants.CODE_SUCCESS);
        } catch (InvalidMessageSizeException e) {
            ResultUtil.addApplicationError(data, response, Constants.CODE_INVALID_SIZE, e.getMessage(), e);
        } catch (TimeoutException e) {
            ResultUtil.addFailure(data, response, Constants.CODE_TIMEOUT, e);
            throw new ConnectorException(Constants.CODE_TIMEOUT, ERROR_MESSAGE_TIMEOUT);
        } catch (ExecutionException e) {
            String errorCode = ErrorCodeHelper.getErrorCode(e);
            if (Constants.CODE_TIMEOUT.equals(errorCode) || !isDynamicUnknownTopic(errorCode)) {
                ResultUtil.addFailure(data, response, errorCode, e);
                throw new ConnectorException(errorCode, ERROR_MESSAGE_TIMEOUT);
            } else {
                ResultUtil.addApplicationError(data, response, errorCode, e.getMessage(), e);
            }
        } catch (ConnectorException e) {
            String errorMessage = ConnectorException.getStatusMessage(e);
            ResultUtil.addApplicationError(data, response, Constants.CODE_ERROR, errorMessage, e);
        } catch (RuntimeException | InterruptedException e) {
            // add the result for the current document
            ResponseUtil.addExceptionFailure(response, data, e);
            // and re throw the Exception
            throw e;
        } finally {
            IOUtil.closeQuietly(message);
        }
    }

    private boolean isDynamicUnknownTopic(String errorCode) {
        return Constants.CODE_UNKNOWN_TOPIC.equals(errorCode) && _isDynamicTopic;
    }

}
