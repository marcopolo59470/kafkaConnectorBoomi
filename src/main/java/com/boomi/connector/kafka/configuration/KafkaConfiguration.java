package com.boomi.connector.kafka.configuration;

import com.boomi.connector.api.ConnectorContext;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.kafka.KafkaConnection;
import com.boomi.connector.kafka.client.common.kerberos.KerberosTicketCache;
import com.boomi.connector.kafka.client.common.kerberos.KerberosTicketKey;
import com.boomi.connector.kafka.client.common.network.BoomiChannelFactory;
import com.boomi.connector.kafka.client.common.serialization.InputStreamDeserializer;
import com.boomi.connector.kafka.client.common.serialization.InputStreamSerializer;
import com.boomi.connector.kafka.operation.KafkaOperationConnection;
import com.boomi.connector.kafka.util.AvroMode;
import com.boomi.connector.kafka.util.Constants;
import com.boomi.connector.util.ConnectorCache;
import com.boomi.connector.util.ConnectorCacheFactory;
import com.boomi.util.ByteUnit;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.net.ssl.SSLContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Base class wrapper for the properties needed to establish a connection with Apache Kafka and configure a Consumer
 * or Producer
 */
public abstract class KafkaConfiguration<T extends AbstractConfig> implements Configuration<T> {
    private static final long MAX_REQUEST_SIZE_PADDING = ByteUnit.byteSize(1, ByteUnit.MB.name());

    protected static final int DEFAULT_TIMEOUT = 30 * 1000;

    private static final int DEFAULT_SESSION_TIMEOUT = 120000;
    private static final int DEFAULT_INTERVAL_HEARTBEAT = 50000;

    private final Map<String, Object> _configs;
    private final Credentials _credentials;
    private final int _maxRequestSize;
    private final ConnectorContext _context;
    private final String _clientId;
    //private final String _avroType;

    protected KafkaConfiguration(KafkaConnection<? extends ConnectorContext> connection) {

        _context = connection.getContext();
        _credentials = Objects.requireNonNull(connection.getCredentials());
        _configs = buildBaseConfiguration(connection.getBootstrapServers(), connection.getSchemaRegistry(), connection.getBasicAuth(), connection.getBasicSource());
        _maxRequestSize = connection.getMaxRequestSize();
        _clientId = connection.getClientId();
        //TODO: change this
        //_avroType = connection.getAvroType().getCode();
        setMaxRequestSize(_maxRequestSize, _configs);

        /**if (Objects.equals(_avroType, "2")) {
            setSerializationWithMessageAndKey(_configs);
        } else if (Objects.equals(_avroType, "1")) {
            setSerializationWithMessage(_configs);
        } else {
            setSerialization(_configs);
        }*/
//setSerializationWithMessage(_configs);

    }



    private static void setMaxRequestSize(int maxRequestSize, Map<String, Object> configs) {
        configs.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, maxRequestSize);
        configs.put(ProducerConfig.BATCH_SIZE_CONFIG, maxRequestSize);
        configs.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, DEFAULT_SESSION_TIMEOUT);
        configs.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, DEFAULT_INTERVAL_HEARTBEAT);
        configs.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, maxRequestSize);
        configs.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxRequestSize);
        //configs.put(AbstractKafkaSchemaSerDeConfig.KEY_SUBJECT_NAME_STRATEGY, _keyStrategy);
        //configs.put(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, _messageStrategy);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getTypeName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getTypeName());
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getTypeName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getTypeName());
    }




    private static Map<String, Object> buildBaseConfiguration(String bootstrapServers, String url, String basic, String source) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // setting max inflight request to 1 as we are not supporting multithreading
        configs.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        // disable retries
        configs.put(CommonClientConfigs.RETRIES_CONFIG, 0);
        configs.put("schema.registry.url", url);
        configs.put("basic.auth.user.info", basic);
        configs.put("basic.auth.credentials.source", source);


        return configs;
    }

    protected final void putConfig(String key, Object value){
        _configs.put(key, value);
    }

    /**
     * Add the non-null configurations present in the given map
     *
     * @param configs map with the key / value configuration
     */
    protected final void putNonNullConfigs(Map<String, Object> configs) {
        configs.forEach((k, v) -> {
            if (v != null) {
                _configs.put(k, v);
            }
        });
    }

    protected final Map<String, Object> getConfigs(){
        return Collections.unmodifiableMap(_configs);
    }

    @Override
    public String getClientId() {
        return _clientId;
    }

    @Override
    public String getUsername() {
        return _credentials.getUsername();
    }

    @Override
    public String getPassword() {
        return _credentials.getPassword();
    }

    @Override
    public SecurityProtocol getSecurityProtocol() {
        return _credentials.getSecurityProtocol();
    }

    @Override
    public SASLMechanism getSaslMechanism() {
        return _credentials.getSaslMechanism();
    }

    @Override
    public String getServiceName() {
        return _credentials.getServiceName();
    }

    @Override
    public ChannelBuilder getChannelBuilder() {
        return BoomiChannelFactory.createChannelBuilder(this);
    }

    @Override
    public SSLContext getSSLContext() {
        return _credentials.getSslContext();
    }

    @Override
    public Map<String, ?> getValues() {
        return Collections.unmodifiableMap(getConfig().values());
    }

    @Override
    public int getMaxRequestSize() {
        long size = _maxRequestSize + MAX_REQUEST_SIZE_PADDING;
        return (int) Math.min(size, Integer.MAX_VALUE);
    }

    /**
     * Get an instance of {@link KerberosTicketCache} containing the Kerberos Service Granting Ticket,
     * based on the configured Client and Service principals.
     *
     * @return the cache instance
     */
    @Override
    public KerberosTicketCache getTicketCache(String hostname) {
        KerberosTicketKey key = new KerberosTicketKey(hostname, getServiceName(), getUsername(), getPassword());
        return ConnectorCache.getCache(key, _context,
                new ConnectorCacheFactory<KerberosTicketKey, KerberosTicketCache, ConnectorContext>() {
                    @Override
                    public KerberosTicketCache createCache(KerberosTicketKey key, ConnectorContext context) {
                        return new KerberosTicketCache(key);
                    }
                });
    }
}
