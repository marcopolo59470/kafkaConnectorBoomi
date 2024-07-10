
package com.boomi.connector.kafka.client.producer;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.kafka.KafkaITContext;
import com.boomi.connector.kafka.TestUtils;
import com.boomi.connector.kafka.client.common.serialization.InputStreamSerializer;
import com.boomi.connector.kafka.configuration.AbstractConfigurationTest;
import com.boomi.connector.kafka.operation.CustomOperationType;
import com.boomi.connector.kafka.operation.KafkaOperationConnection;
import com.boomi.connector.kafka.util.Constants;
import com.boomi.util.ByteUnit;
import com.boomi.util.StringUtil;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(Theories.class)
public class ProduceConfigurationTest extends AbstractConfigurationTest {

    @DataPoints(value = "invalidClientIds")
    public static final String[] INVALID_CLIENT_IDS = new String[] { StringUtil.EMPTY_STRING, null };

    private static final long MAX_REQUEST_SIZE_PADDING = ByteUnit.byteSize(1, ByteUnit.MB.name());
    private static final long EXPECTED_MAX_WAIT_TIMEOUT = 42L;
    private static final String EXPECTED_COMPRESSION_TYPE = "snappy";
    private static final String EXPECTED_ACKS = "-1";
    private static final String EXPECTED_CLIENT_ID = "producer client id";

    private static void addDefaultProperties(KafkaITContext context) {
        context.setOperationCustomType(CustomOperationType.PRODUCE.name());
        context.setOperationType(OperationType.EXECUTE);

        context.addOperationProperty(Constants.KEY_MAXIMUM_TIME_TO_WAIT, EXPECTED_MAX_WAIT_TIMEOUT);
        context.addOperationProperty(Constants.KEY_COMPRESSION_TYPE, EXPECTED_COMPRESSION_TYPE);
        context.addOperationProperty(Constants.KEY_ACKS, EXPECTED_ACKS);
        context.addOperationProperty(Constants.KEY_CLIENT_ID, EXPECTED_CLIENT_ID);
    }

    private static void assertConsumerPropertiesNotPresent(ProducerConfiguration configuration) {
        Map<String, ?> values = configuration.getValues();
        Assert.assertNotNull(values);

        Assert.assertNull(values.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
        Assert.assertNull(values.get(ConsumerConfig.FETCH_MAX_BYTES_CONFIG));
        Assert.assertNull(values.get(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG));
        Assert.assertNull(values.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
        Assert.assertNull(values.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    }

    private static void assertDefaultProperties(ProducerConfiguration configuration) {
        assertEquals(EXPECTED_CLIENT_ID, configuration.getClientId());

        Map<String, ?> values = configuration.getValues();
        Assert.assertNotNull(values);

        //Version 3.2 org.apache.kafka.clients.producer.ProducerConfig.parseAcks All is parsed to -1
        assertEquals(EXPECTED_ACKS, values.get(ProducerConfig.ACKS_CONFIG));
        assertEquals(EXPECTED_COMPRESSION_TYPE, values.get(ProducerConfig.COMPRESSION_TYPE_CONFIG));
        assertEquals((int) EXPECTED_MAX_WAIT_TIMEOUT, values.get(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG));
        assertEquals(EXPECTED_MAX_WAIT_TIMEOUT, values.get(ProducerConfig.MAX_BLOCK_MS_CONFIG));
    }

    @Test
    public void produceConfigurationTest() {
        KafkaITContext context = getContextDouble();
        addDefaultProperties(context);

        KafkaOperationConnection connection = TestUtils.createOperationConnection(context);

        // execute SUT
        ProducerConfiguration configuration = ProducerConfiguration.create(connection);

        // asserts
        assertEquals((int) EXPECTED_MAX_WAIT_TIMEOUT, configuration.getMaxWaitTimeout());

        Assert.assertNotNull(configuration.getConfig());

        assertCredentials(configuration);
        assertDefaultProperties(configuration);
        assertConsumerPropertiesNotPresent(configuration);

        Map<String, ?> values = configuration.getValues();

        // container properties
        assertEquals(DEFAULT_MAX_FETCH_SIZE + MAX_REQUEST_SIZE_PADDING, configuration.getMaxRequestSize());
        assertEquals(DEFAULT_MAX_FETCH_SIZE, values.get(ProducerConfig.BATCH_SIZE_CONFIG));
        assertEquals(DEFAULT_MAX_FETCH_SIZE, values.get(ProducerConfig.MAX_REQUEST_SIZE_CONFIG));

        // fixed values
        assertEquals(InputStreamSerializer.class, values.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        assertEquals(StringSerializer.class, values.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals(1, values.get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION));
    }

    @Theory
    public void shouldFailWithBlankClientIDTest(@FromDataPoints("invalidClientIds") String clientId) {
        KafkaITContext context = getContextDouble();

        context.addOperationProperty(Constants.KEY_RECEIVE_MESSAGE_TIMEOUT, EXPECTED_MAX_WAIT_TIMEOUT);
        context.addOperationProperty(Constants.KEY_COMPRESSION_TYPE, EXPECTED_COMPRESSION_TYPE);
        context.addOperationProperty(Constants.KEY_ACKS, EXPECTED_ACKS);
        context.addOperationProperty(Constants.KEY_CLIENT_ID, clientId);

        KafkaOperationConnection connection = TestUtils.createOperationConnection(context);

        try {
            ProducerConfiguration.create(connection);
        } catch (ConnectorException ignored) {
            return;
        }

        Assert.fail("should have thrown a ConnectorException");
    }

    @Test
    public void consumeConfigurationWithCustomContainerPropertiesTest() {
        KafkaITContext context = getContextDouble();
        addDefaultProperties(context);

        final int size = 42;
        context.withContainerProperty(Constants.KEY_MAX_POLL_RECORDS, String.valueOf(1)).withContainerProperty(
                Constants.KEY_MAX_FETCH_SIZE, String.valueOf(size));

        KafkaOperationConnection connection = TestUtils.createOperationConnection(context);

        // build SUT
        ProducerConfiguration configuration = ProducerConfiguration.create(connection);

        // asserts
        assertDefaultProperties(configuration);
        assertCredentials(configuration);
        assertConsumerPropertiesNotPresent(configuration);

        assertEquals(size + MAX_REQUEST_SIZE_PADDING, configuration.getMaxRequestSize());
        assertEquals(size, configuration.getValues().get(ProducerConfig.BATCH_SIZE_CONFIG));
        assertEquals(size, configuration.getValues().get(ProducerConfig.MAX_REQUEST_SIZE_CONFIG));
    }
}
