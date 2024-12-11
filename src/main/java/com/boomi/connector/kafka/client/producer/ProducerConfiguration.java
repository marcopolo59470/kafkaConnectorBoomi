package com.boomi.connector.kafka.client.producer;

import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.kafka.client.common.serialization.InputStreamSerializer;
import com.boomi.connector.kafka.configuration.KafkaConfiguration;
import com.boomi.connector.kafka.operation.KafkaOperationConnection;
import static com.boomi.connector.kafka.util.Tools.translateEscapes;

import com.boomi.connector.kafka.operation.produce.SSLCredentials;
import com.boomi.connector.kafka.util.AvroMode;
import com.boomi.connector.kafka.util.Constants;
import com.boomi.util.LogUtil;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.MessageFormat;
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
     * @param connection        a KafkaConnection<BrowseContext> instance
     * @param dynamicProperties
     */
    public ProducerConfiguration(KafkaOperationConnection connection, SSLCredentials dynamicProperties) {
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

        //Authentification
        putConfig(SchemaRegistryClientConfig.USER_INFO_CONFIG, getDynamicIfPresent(
                dynamicProperties.getCredentialSource(), properties.getProperty(Constants.BASIC_AUTH_USER_INFO)));
        putConfig(SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE, translateEscapes(properties.getProperty(Constants.BASIC_AUTH_CREDENTIALS_SOURCE)));
        putConfig(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, getDynamicIfPresent(
                dynamicProperties.getBootstrapServer(), properties.getProperty(Constants.BOOTSTRAP_SERVER)));
        putConfig(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        putConfig(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG,"PEM");
        putConfig(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,"PEM");


        //key
        putConfig(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, getDynamicIfPresent(
                dynamicProperties.getAccessKey(), properties.getProperty(Constants.ACCESS_KEY)));
        //cert
        putConfig(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, getDynamicIfPresent(
                dynamicProperties.getAccessCertificate(), properties.getProperty(Constants.ACCESS_CERT)));
        //pem
        putConfig(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, getDynamicIfPresent(
                dynamicProperties.getCACertificate(), properties.getProperty(Constants.CA_CERTIFICATE)));


        if (Objects.equals(_avroType, "2")) {
            putConfig("schema.registry.url", getDynamicIfPresent(dynamicProperties.getSchemaUrl(), properties.getProperty(Constants.SCHEMA_REGISTRY_URL)));
            putConfig(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getTypeName());
            putConfig(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getTypeName());
        } else if (Objects.equals(_avroType, "1")) {
            putConfig("schema.registry.url", getDynamicIfPresent(dynamicProperties.getSchemaUrl(), properties.getProperty(Constants.SCHEMA_REGISTRY_URL)));
            putConfig(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getTypeName());
            putConfig(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getTypeName());
        } else {
            putConfig(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getTypeName());
            putConfig(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, InputStreamSerializer.class.getTypeName());
        }

    }

    private String getDynamicIfPresent(String dynamic, String operation) {
        return translateEscapes((dynamic != null && !dynamic.isEmpty()) ? dynamic : operation);
    }

    private AvroMode getAvroType(KafkaOperationConnection connection) {
        String mode = connection.getContext().getOperationProperties().getProperty(Constants.KEY_AVRO_MODE);

        //LOG.log(Level.INFO, AvroMode.getByCode(mode).toString());
        return (mode == null || mode.isEmpty()) ? AvroMode.NO_MESSAGE : AvroMode.getByCode(mode);
    }

    /**
     * Creates the configuration necessary for Produce Operation.
     *
     * @param connection        a KafkaConnection<BrowseContext> instance
     * @param dynamicProperties
     */
    public static ProducerConfiguration create(KafkaOperationConnection connection, SSLCredentials dynamicProperties) {
        return new ProducerConfiguration(connection, dynamicProperties);
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
