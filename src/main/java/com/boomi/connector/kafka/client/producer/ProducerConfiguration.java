package com.boomi.connector.kafka.client.producer;

import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.kafka.client.common.serialization.InputStreamDeserializer;
import com.boomi.connector.kafka.client.common.serialization.InputStreamSerializer;
import com.boomi.connector.kafka.configuration.KafkaConfiguration;
import com.boomi.connector.kafka.operation.KafkaOperationConnection;
import com.boomi.connector.kafka.util.AvroMode;
import com.boomi.connector.kafka.util.Constants;
import com.boomi.util.LogUtil;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.MessageFormat;
import java.util.Map;
import java.util.Objects;
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
        String _avroType = getAvroType(connection).getCode();
        PropertyMap properties = connection.getContext().getOperationProperties();

        putConfig(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, _maxWaitTimeout);
        putConfig(ProducerConfig.MAX_BLOCK_MS_CONFIG, _maxWaitTimeout);
        putConfig(ProducerConfig.ACKS_CONFIG, properties.getProperty(Constants.KEY_ACKS));
        putConfig(ProducerConfig.COMPRESSION_TYPE_CONFIG, properties.getProperty(Constants.KEY_COMPRESSION_TYPE));
        putConfig(AbstractKafkaSchemaSerDeConfig.KEY_SUBJECT_NAME_STRATEGY, properties.getProperty(Constants.KEY_SUBJECT_NAME_STRATEGY));
        putConfig(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, properties.getProperty(Constants.VALUE_SUBJECT_NAME_STRATEGY));

        if (Objects.equals(_avroType, "2")) {
            putConfig(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getTypeName());
            putConfig(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getTypeName());
        } else if (Objects.equals(_avroType, "1")) {
            putConfig(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getTypeName());
            putConfig(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getTypeName());
        } else {
            putConfig(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getTypeName());
            putConfig(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, InputStreamSerializer.class.getTypeName());
        }

    }

    private AvroMode getAvroType(KafkaOperationConnection connection) {
        String mode = connection.getContext().getOperationProperties().getProperty(Constants.KEY_AVRO_MODE);

        //LOG.log(Level.INFO, AvroMode.getByCode(mode).toString());
        return (mode == null || mode.isEmpty()) ? AvroMode.NO_MESSAGE : AvroMode.getByCode(mode);
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
