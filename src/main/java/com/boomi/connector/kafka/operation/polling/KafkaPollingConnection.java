package com.boomi.connector.kafka.operation.polling;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.PrivateKeyStore;
import com.boomi.connector.kafka.client.consumer.BoomiCustomConsumer;
import com.boomi.connector.kafka.operation.KafkaOperationConnection;
import com.boomi.connector.kafka.util.Constants;
import com.boomi.util.NumberUtil;

import java.util.function.Supplier;

/**
 * Implementation of {@link com.boomi.connector.util.BaseConnection<OperationContext>} that provides access to the
 * operation properties required by {@link KafkaPollingOperation}.
 */
public class KafkaPollingConnection extends KafkaOperationConnection {

    public KafkaPollingConnection(OperationContext context, PrivateKeyStore pks) {
        super(context, pks);
    }

    BoomiListenerConsumer createPollingConsumer(String dynamicRegexTopicValue, String topic) {
        if (isRegexTopic()){
            return new BoomiListenerConsumer(createSupplierRegex(regexTopicValue(dynamicRegexTopicValue)));
        }

        Supplier<BoomiCustomConsumer> supplier = isAssignPartitions() ? createSupplier(topic, getPartitionsIds())
                : createSupplier(topic);
        return new BoomiListenerConsumer(supplier);
    }

    @Override
    public int getMaxPollRecords() {
        Long propValue = getContext().getOperationProperties().getLongProperty(Constants.KEY_MAX_MESSAGES);
        int maxPollRecords = NumberUtil.toInteger(propValue, 0);

        return validateIsGreaterThanOne(maxPollRecords, "Maximum Number of Messages per poll");
    }

    /**
     * Get the configured polling delay for this connection expressed in milliseconds.
     *
     * @return the polling delay
     */
    long getPollingDelay() {
        // a default value is provided for old saved connections without this configuration
        long property = getContext().getConnectionProperties().getLongProperty(Constants.KEY_POLLING_DELAY,
                Constants.DEFAULT_POLLING_DELAY);
        if (property < 0) {
            throw new ConnectorException("Polling Delay cannot be less than 0");
        }

        return property;
    }

    /**
     * Get the configured polling interval for this connection.
     *
     * @return the interval
     */
    long getPollingInterval() {
        // a default value is provided for old saved connections without this configuration
        long pollInterval = getContext().getConnectionProperties().getLongProperty(Constants.KEY_POLLING_INTERVAL,
                Constants.DEFAULT_POLLING_INTERVAL);
        if (pollInterval < 1) {
            throw new ConnectorException("Polling Interval cannot be less than 1");
        }

        return pollInterval;
    }

    /**public SSLContext getPrivateCertificate() {
        SSLContextFactory sslContextFactory = new SSLContextFactory();
        PrivateKeyStore certificate = getContext().getConnectionProperties().getPrivateKeyStoreProperty(Constants.KEY_CERTIFICATE_OPERATION);
        return sslContextFactory.create(certificate);
    }*/

    String getConsumerGroup() {
        return getContext().getOperationProperties().getProperty(Constants.KEY_CONSUMER_GROUP);
    }

    /**
     * Get the configured singleton listener from the Listen operation.
     *
     * @return true if singleton listener is checked. Otherwise, false is returned.
     */
    boolean isSingletonListener() {
        return getContext().getOperationProperties().getBooleanProperty(Constants.KEY_SINGLETON_LISTENER, false);
    }

}
