
package com.boomi.connector.kafka.operation.consume;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.kafka.client.consumer.ConsumeMessage;
import com.boomi.connector.kafka.exception.CommitOffsetException;
import com.boomi.connector.kafka.operation.KafkaOperationConnection;
import com.boomi.connector.kafka.util.Constants;
import com.boomi.connector.kafka.util.TopicNameUtil;
import com.boomi.connector.util.BaseUpdateOperation;
import com.boomi.util.IOUtil;

import org.apache.kafka.common.network.InvalidReceiveException;

import java.util.logging.Level;

/**
 * Execute Operation for retrieving messages from Apache Kafka. Gets messages from the Topic specified as Object Type
 * until the timeout is reached or the minimum amount of messages is returned.
 */
public class ConsumeOperation extends BaseUpdateOperation {

    private final String _topic;
    private final boolean _isAutocommit;
    private final long _minMessages;
    private final long _maxExecutionTime;

    public ConsumeOperation(KafkaOperationConnection connection) {
        super(connection);
        _topic = TopicNameUtil.getTopic(connection.getObjectTypeId(),connection.getContext());

        PropertyMap properties = connection.getContext().getOperationProperties();
        _isAutocommit = isAutocommit(properties);
        _minMessages = getMinMessages(properties);
        _maxExecutionTime = getMaxWaitTimeout(properties);
    }

    private static boolean isAutocommit(PropertyMap propertyMap) {
        return propertyMap.getBooleanProperty(Constants.KEY_AUTOCOMMIT, false);
    }

    private static long getMinMessages(PropertyMap propertyMap) {
        return propertyMap.getLongProperty(Constants.KEY_MIN_MESSAGES, 0L);
    }

    private static long getMaxWaitTimeout(PropertyMap propertyMap) {
        long value = propertyMap.getLongProperty(Constants.KEY_RECEIVE_MESSAGE_TIMEOUT, -1L);
        if (value < 1) {
            throw new ConnectorException("Receive Message Timeout cannot be empty or less than 1");
        }
        return value;
    }

    @Override
    public KafkaOperationConnection getConnection() {
        return (KafkaOperationConnection) super.getConnection();
    }

    @Override
    protected void executeUpdate(UpdateRequest request, OperationResponse response) {
        ConsumeResponseHandler responseHandler = new ConsumeResponseHandler(request, response);

        BoomiConsumer consumer = null;
        try {
            consumer = getConnection().createConsumer(_topic);
            executeConsume(consumer, responseHandler);
        } catch (Exception e) {
            responseHandler.addFailure(e, Constants.CODE_ERROR);
        } finally {
            responseHandler.finish();
            IOUtil.closeQuietly(consumer);
        }
    }

    private void executeConsume(BoomiConsumer consumer, ConsumeResponseHandler responseHandler) {

        final long executionEndTime = _maxExecutionTime + System.currentTimeMillis();

        long receivedMsgs = 0L;
        long remainingTime = _maxExecutionTime;

        do {
            try {
                receivedMsgs += processMessages(responseHandler, consumer, remainingTime);
            } catch (InvalidReceiveException e) {
                responseHandler.log(Level.SEVERE, e.getMessage(), e);
                processError(responseHandler, consumer, e);

                if (!_isAutocommit) {
                    return;
                }

                // creates a new consumer as the previous one is no longer valid to retrieve messages
                consumer.reSubscribe();
                receivedMsgs++;
            }

            remainingTime = executionEndTime - System.currentTimeMillis();
        } while ((remainingTime > 0) && hasRemainingMessages(receivedMsgs));
    }

    private boolean hasRemainingMessages(long received) {
        return (_minMessages <= 0) || (received < _minMessages);
    }

    private long processMessages(ConsumeResponseHandler responseHandler, BoomiConsumer consumer, long timeout) {
        long msgCount = 0L;
        for (ConsumeMessage message : consumer.poll(timeout)) {
            try {
                if (_isAutocommit) {
                    consumer.commit(message);
                }
                responseHandler.addSuccess(message.toPayload(getContext().createMetadata()));
            } catch (CommitOffsetException e) {
                responseHandler.log(Level.WARNING, e.getMessage(), e);
                responseHandler.addApplicationError(message.toPayload(getContext().createMetadata()),
                        "The message could not be committed", Constants.CODE_ERROR);
            } finally {
                IOUtil.closeQuietly(message);
            }

            msgCount++;
        }
        return msgCount;
    }

    private void processError(ConsumeResponseHandler responseHandler, BoomiConsumer consumer,
            InvalidReceiveException e) {
        ConsumeMessage errorMsg = null;

        try {
            errorMsg = consumer.getLastErrorMessage();

            // if there is no error message, propagate the exception to add it as a Failure
            if (errorMsg == null) {
                throw e;
            }

            PayloadMetadata metadata = getContext().createMetadata();
            responseHandler.addApplicationError(errorMsg.toPayload(metadata), e.getMessage(), Constants.CODE_ERROR);
            if (_isAutocommit) {
                consumer.commit(errorMsg);
            }
        } catch (CommitOffsetException ex) {
            responseHandler.log(Level.WARNING, ex.getMessage(), ex);
        } finally {
            IOUtil.closeQuietly(errorMsg);
        }
    }
}
