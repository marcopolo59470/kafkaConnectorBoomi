package com.boomi.connector.kafka.client.consumer;

import com.boomi.connector.api.ConnectorContext;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.kafka.KafkaConnection;
import com.boomi.connector.kafka.client.common.serialization.InputStreamSerializer;
import com.boomi.connector.kafka.configuration.KafkaConfiguration;
import com.boomi.connector.kafka.operation.CustomOperationType;
import com.boomi.connector.kafka.operation.KafkaOperationConnection;
import com.boomi.connector.kafka.util.AvroMode;
import com.boomi.connector.kafka.util.Constants;
import com.boomi.util.StringUtil;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Wrapper for the properties needed to establish a connection with Apache Kafka and configure a Consumer
 */
public class ConsumerConfiguration extends KafkaConfiguration<ConsumerConfig> {

    /**
     * Constructs the configuration necessary for Test Connection and Browse
     *
     * @param connection
     *         a KafkaConnection<BrowseContext> instance
     */
    private ConsumerConfiguration(KafkaConnection<? extends ConnectorContext> connection, int timeout) {
        super(connection);
        putConfig(ConsumerConfig.GROUP_ID_CONFIG, getConnectionConsumerGroup(connection));
        putConfig(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, timeout);

    }

    private AvroMode getAvroType(KafkaConnection<OperationContext> connection) {
        String mode = connection.getContext().getOperationProperties().getProperty(Constants.KEY_AVRO_MODE);

        //LOG.log(Level.INFO, AvroMode.getByCode(mode).toString());
        return (mode == null || mode.isEmpty()) ? AvroMode.NO_MESSAGE : AvroMode.getByCode(mode);
    }

    /**
     * Constructs the configuration necessary for Consume Operation
     *
     * @param connection
     *         a KafkaOperationConnection instance
     */
    protected ConsumerConfiguration(KafkaConnection<OperationContext> connection) {
        super(connection);
        putConfig(ConsumerConfig.GROUP_ID_CONFIG, getConnectionConsumerGroup(connection));
        putNonNullConfigs(getOpConfig(connection));
        validateConsumerGroup(getConfigs());

        String _avroType = getAvroType(connection).getCode();

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

    /**
     * Creates the configuration necessary for Test Connection and Browse
     *
     * @param connection
     *         a KafkaConnection<BrowseContext> instance
     */
    public static ConsumerConfiguration browse(KafkaConnection<? extends ConnectorContext> connection){
        return new ConsumerConfiguration(connection, DEFAULT_TIMEOUT);
    }

    /**
     * Creates the configuration necessary for Consume & Listen Operation
     *
     * @param connection
     *         a KafkaOperationConnection instance
     */
    public static ConsumerConfiguration consumer(KafkaConnection<OperationContext>  connection){
        return new ConsumerConfiguration(connection);
    }

    private static Map<String, Object> getOpConfig(KafkaConnection<OperationContext> connection) {
        Map<String, Object> configs = new HashMap<>();
        PropertyMap properties = connection.getContext().getOperationProperties();

        String consumerGroup = properties.getProperty(Constants.KEY_CONSUMER_GROUP);
        if (StringUtil.isNotBlank(consumerGroup)) {
            configs.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        }

        if (CustomOperationType.COMMIT_OFFSET != CustomOperationType.fromContext(connection.getContext())) {
            String autoOffsetReset = properties.getProperty(Constants.KEY_AUTO_OFFSET_RESET);
            configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        }

        configs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, connection.getMaxPollRecords());
        // The connector explicitly commits the last offset so this property is always disabled.
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return configs;
    }

    /**
     * Extract the consumer group configured in the given connection, or null if not present
     *
     * @param connection to extract the consumer group
     * @return the consumer group value or null
     */
    private static String getConnectionConsumerGroup(KafkaConnection<? extends ConnectorContext> connection) {
        ConnectorContext context = connection.getContext();
        PropertyMap properties = context.getConnectionProperties();
        return properties.getProperty(Constants.KEY_CONSUMER_GROUP);
    }

    /**
     * Validate that the given configuration map contains an entry for {@link ConsumerConfig#GROUP_ID_CONFIG} that is
     * not null nor blank
     *
     * @param config map with the key / value configuration
     * @throws ConnectorException if {@link ConsumerConfig#GROUP_ID_CONFIG} configuration is not present
     */
    private static void validateConsumerGroup(Map<String, Object> config) {
        Object consumerGroup = config.get(ConsumerConfig.GROUP_ID_CONFIG);
        if (consumerGroup == null || StringUtil.isBlank(String.valueOf(consumerGroup))) {
            throw new ConnectorException("Consumer Group cannot be blank");
        }
    }

    @Override
    public ConsumerConfig getConfig() {
        return new ConsumerConfig(getConfigs());
    }
}
