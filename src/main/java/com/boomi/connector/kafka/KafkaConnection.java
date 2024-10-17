package com.boomi.connector.kafka;

import com.boomi.connector.api.AtomConfig;
import com.boomi.connector.api.ConnectorContext;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.PrivateKeyStore;
import com.boomi.connector.kafka.client.consumer.BoomiCustomConsumer;
import com.boomi.connector.kafka.client.consumer.ConsumerConfiguration;
import com.boomi.connector.kafka.configuration.Credentials;
import com.boomi.connector.kafka.util.AvroMode;
import com.boomi.connector.kafka.util.Constants;
import com.boomi.connector.util.BaseConnection;
import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;
import com.boomi.util.NumberUtil;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.network.InvalidReceiveException;

import java.io.InputStream;
import java.text.MessageFormat;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KafkaConnection<C extends ConnectorContext> extends BaseConnection<C> {

    private static final Logger LOG = LogUtil.getLogger(KafkaConnector.class);
    private static final String DEFAULT_CLIENT_ID = "Boomi Connector";
    private static final String INVALID_SIZE_MESSAGE = "{0}. Check if you are using a PLAIN protocol instead of SSL";

    private final Credentials _credentials;

    public KafkaConnection(C context, PrivateKeyStore pks) {
        super(context);
        _credentials = new Credentials(context, pks);
    }

    /**public SSLContext getPrivateCertificate() {
        SSLContextFactory sslContextFactory = new SSLContextFactory();
        PrivateKeyStore certificate = getContext().getConnectionProperties().getPrivateKeyStoreProperty(Constants.KEY_CERTIFICATE_OPERATION);
        return sslContextFactory.create(certificate);
    }*/
    /**
     * Perform a request to Kafka in order to retrieve a {@link Set} of the available topics
     *
     * @return a Set of the available topics
     */
    Set<String> getTopics() {
        Consumer<Object, InputStream> consumer = null;

        try {
            consumer = new BoomiCustomConsumer(ConsumerConfiguration.browse(this));
            return consumer.listTopics().keySet();
        } catch (InvalidReceiveException e) {
            String message = e.getMessage();
            LOG.log(Level.SEVERE, message, e);
            throw new ConnectorException(MessageFormat.format(INVALID_SIZE_MESSAGE, message));
        } finally {
            IOUtil.closeQuietly(consumer);
        }
    }

    public String getClientId() {
        return DEFAULT_CLIENT_ID;
    }


    public String getKeyStrategy() {
        return getContext().getConnectionProperties().getProperty(Constants.KEY_SUBJECT_NAME_STRATEGY);
    }

    public String getMessageStrategy() {
        return getContext().getConnectionProperties().getProperty(Constants.VALUE_SUBJECT_NAME_STRATEGY);
    }

    /**public AvroMode getAvroType() {
        String mode = getContext().getConnectionProperties().getProperty(Constants.KEY_AVRO_MODE);

        //LOG.log(Level.INFO, AvroMode.getByCode(mode).toString());
        return (mode == null || mode.isEmpty()) ? AvroMode.NO_MESSAGE : AvroMode.getByCode(mode);
    }*/
    public Credentials getCredentials() {
        return _credentials;
    }

    public String getBootstrapServers() {
        return getContext().getConnectionProperties().getProperty(Constants.KEY_SERVERS);
    }

    public String getSchemaRegistry() {
        return getContext().getConnectionProperties().getProperty(Constants.SCHEMA_REGISTRY_URL);
    }

    public String getBasicAuth() {
        return getContext().getConnectionProperties().getProperty(Constants.BASIC_AUTH_USER_INFO);
    }

    public String getBasicSource() {
        return getContext().getConnectionProperties().getProperty(Constants.BASIC_AUTH_CREDENTIALS_SOURCE);
    }

    public int getMaxRequestSize() {
        int maxRequestSize = getContainerProperty(Constants.KEY_MAX_FETCH_SIZE, Constants.DEFAULT_MAX_FETCH_SIZE);
        return validateIsGreaterThanOne(maxRequestSize, Constants.KEY_MAX_POLL_RECORDS);
    }

    public int getMaxPollRecords() {
        int maxPollRecords = getContainerProperty(Constants.KEY_MAX_POLL_RECORDS, Constants.DEFAULT_MAX_POLL_RECORDS);
        return validateIsGreaterThanOne(maxPollRecords, Constants.KEY_MAX_POLL_RECORDS);
    }

    private int getContainerProperty(String key, int defaultValue) {
        AtomConfig config = getContext().getConfig();
        if (config == null) {
            return defaultValue;
        }

        return NumberUtil.toInteger(config.getContainerProperty(key), defaultValue);
    }

    protected int validateIsGreaterThanOne(int value, String label) {
        if (value < 1) {
            throw new ConnectorException(label + " cannot be less than 1");
        }

        return value;
    }

    static String defaultValueIfNullOrBlank(String value, String defaultValue) {
        return (value == null || value.isBlank()) ? defaultValue : value;
    }
}
