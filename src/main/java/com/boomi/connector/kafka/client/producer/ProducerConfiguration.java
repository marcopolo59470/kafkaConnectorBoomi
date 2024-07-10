package com.boomi.connector.kafka.client.producer;

import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.kafka.configuration.KafkaConfiguration;
import com.boomi.connector.kafka.operation.KafkaOperationConnection;
import com.boomi.connector.kafka.util.Constants;
import com.boomi.util.LogUtil;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.text.MessageFormat;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Wrapper for the properties needed to establish a connection with Apache Kafka and configure a Producer.
 */
public class ProducerConfiguration extends KafkaConfiguration<ProducerConfig> {

    private static final Logger LOG = LogUtil.getLogger(ProducerConfiguration.class);
    private static final String TIMEOUT_TOO_LONG_MESSAGE_FORMAT =
            "The value {0} configured for Maximum Time to Wait in the operation is too long, it will be set to {1}.";
    private final int _maxWaitTimeout;

    /**
     * Constructs the configuration necessary for Produce Operation.
     *
     * @param connection
     *         a KafkaConnection<BrowseContext> instance
     */
    public ProducerConfiguration(KafkaOperationConnection connection) {
        super(connection);
        _maxWaitTimeout = getTimeout(connection);
        PropertyMap properties = connection.getContext().getOperationProperties();

        putConfig(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, _maxWaitTimeout);
        putConfig(ProducerConfig.MAX_BLOCK_MS_CONFIG, _maxWaitTimeout);
        putConfig(ProducerConfig.ACKS_CONFIG, properties.getProperty(Constants.KEY_ACKS));
        putConfig(ProducerConfig.COMPRESSION_TYPE_CONFIG, properties.getProperty(Constants.KEY_COMPRESSION_TYPE));
    }

    /**
     * Creates the configuration necessary for Produce Operation.
     *
     * @param connection
     *         a KafkaConnection<BrowseContext> instance
     */
    public static ProducerConfiguration create(KafkaOperationConnection connection) {
        return new ProducerConfiguration(connection);
    }

    /**
     * If the timeout set is lower than Integer maximum possible value it returns the timeout, else returns Integer
     * maximum possible value.
     *
     * @param connection
     *         a KafkaOperationConnection to get the timeout
     * @return timeout
     */
    private static int getTimeout(KafkaOperationConnection connection) {
        PropertyMap properties = connection.getContext().getOperationProperties();
        long timeout = properties.getLongProperty(Constants.KEY_MAXIMUM_TIME_TO_WAIT, (long) DEFAULT_TIMEOUT);

        if (timeout < Integer.MAX_VALUE) {
            return (int) timeout ;
        } else {
            String message = MessageFormat.format(TIMEOUT_TOO_LONG_MESSAGE_FORMAT, timeout, Integer.MAX_VALUE);
            LOG.log(Level.WARNING, message);
            return Integer.MAX_VALUE;
        }
    }

    @Override
    public ProducerConfig getConfig() {
        return new ProducerConfig(getConfigs());
    }

    long getMaxWaitTimeout() {
        return _maxWaitTimeout;
    }
}
