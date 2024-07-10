
package com.boomi.connector.kafka.client.consumer;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.kafka.KafkaConnection;
import com.boomi.connector.kafka.KafkaITContext;
import com.boomi.connector.kafka.TestUtils;
import com.boomi.connector.kafka.configuration.AbstractConfigurationTest;
import com.boomi.connector.kafka.operation.CustomOperationType;
import com.boomi.connector.kafka.operation.KafkaOperationConnection;
import com.boomi.connector.kafka.util.Constants;
import com.boomi.util.ByteUnit;
import com.boomi.util.StringUtil;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.util.Map;

@RunWith(Theories.class)
public class ConsumerConfigurationTest extends AbstractConfigurationTest {

    @DataPoints(value = "invalidClientIds")
    public static final String[] INVALID_CLIENT_IDS = new String[] { StringUtil.EMPTY_STRING, null };

    @DataPoints(value = "autoResetOffset")
    public static final String[] AUTO_OFFSET_RESETS = new String[] { "earliest", "latest" };

    @DataPoints(value = "consumerGroups")
    // connection value | operation value | expected value
    public static final String[][] CONSUMER_GROUPS = new String[][] {
            { "connectionGroup", StringUtil.EMPTY_STRING, "connectionGroup" },
            { StringUtil.EMPTY_STRING, "operationGroup", "operationGroup" },
            { "connectionGroup", "operationGroup", "operationGroup" } };

    private static final long MAX_REQUEST_SIZE_PADDING = ByteUnit.byteSize(1, ByteUnit.MB.name());
    private static final long MIN_MESSAGES = 42L;
    private static final long MAX_WAIT_TIMEOUT = 43L;
    private static final boolean AUTOCOMMIT = true;
    private static final String CONSUMER_GROUP = "default-consumer-group-it";
    private static final String CLIENT_ID = "default-client-id-it";
    private static final String AUTO_OFFSET_RESET = "earliest";

    private static void addDefaultConsumerProperties(KafkaITContext context) {
        context.setOperationType(OperationType.EXECUTE);
        context.setOperationCustomType(CustomOperationType.CONSUME.name());

        context.addOperationProperty(Constants.KEY_CONSUMER_GROUP, CONSUMER_GROUP);
        context.addOperationProperty(Constants.KEY_CLIENT_ID, CLIENT_ID);
        context.addOperationProperty(Constants.KEY_AUTOCOMMIT, AUTOCOMMIT);
        context.addOperationProperty(Constants.KEY_MIN_MESSAGES, MIN_MESSAGES);
        context.addOperationProperty(Constants.KEY_RECEIVE_MESSAGE_TIMEOUT, MAX_WAIT_TIMEOUT);
        context.addOperationProperty(Constants.KEY_AUTO_OFFSET_RESET, AUTO_OFFSET_RESET);
    }

    private static void assertProducerPropertiesNotPresent(ConsumerConfiguration configuration) {
        Map<String, ?> properties = configuration.getConfig().values();

        Assert.assertNull(properties.get(ProducerConfig.BATCH_SIZE_CONFIG));
        Assert.assertNull(properties.get(ProducerConfig.MAX_REQUEST_SIZE_CONFIG));
        Assert.assertNull(properties.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        Assert.assertNull(properties.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        Assert.assertNull(properties.get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION));
    }

    private static void assertDefaultConsumerProperties(ConsumerConfiguration configuration) {
        Assert.assertEquals(CLIENT_ID, configuration.getClientId());

        Map<String, ?> values = configuration.getValues();
        ConsumerConfig config = configuration.getConfig();

        Assert.assertEquals(CONSUMER_GROUP, values.get(ConsumerConfig.GROUP_ID_CONFIG));
        Assert.assertEquals(CONSUMER_GROUP, config.values().get(ConsumerConfig.GROUP_ID_CONFIG));

        // this properties are fixed and should always have the same values
        Assert.assertFalse("Autocommit should be disabled",
                ((Boolean) values.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)));
        Assert.assertFalse("Autocommit should be disabled",
                ((Boolean) config.values().get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)));

        Assert.assertEquals(AUTO_OFFSET_RESET, config.values().get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        Assert.assertEquals(AUTO_OFFSET_RESET, values.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    }

    @Theory
    public void consumeConfigurationTest(@FromDataPoints("autoResetOffset") String autoOffsetReset) {
        KafkaITContext context = getContextDouble();
        addDefaultConsumerProperties(context);

        context.addOperationProperty(Constants.KEY_AUTO_OFFSET_RESET, autoOffsetReset);

        // build SUT
        ConsumerConfiguration configuration = createFromContext(context);

        // asserts
        assertCredentials(configuration);
        assertProducerPropertiesNotPresent(configuration);

        Assert.assertEquals(CLIENT_ID, configuration.getClientId());

        Map<String, ?> values = configuration.getValues();
        ConsumerConfig config = configuration.getConfig();

        Assert.assertEquals(CONSUMER_GROUP, values.get(ConsumerConfig.GROUP_ID_CONFIG));
        Assert.assertEquals(CONSUMER_GROUP, config.values().get(ConsumerConfig.GROUP_ID_CONFIG));

        // this properties are fixed and should always have the same values
        Assert.assertFalse("Autocommit should be disabled",
                ((Boolean) values.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)));
        Assert.assertFalse("Autocommit should be disabled",
                ((Boolean) config.values().get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)));

        Assert.assertEquals(autoOffsetReset, config.values().get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        Assert.assertEquals(autoOffsetReset, values.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));

        // container properties
        Assert.assertEquals(DEFAULT_MAX_FETCH_SIZE + MAX_REQUEST_SIZE_PADDING, configuration.getMaxRequestSize());
        Assert.assertEquals(DEFAULT_MAX_FETCH_SIZE,
                configuration.getValues().get(ConsumerConfig.FETCH_MAX_BYTES_CONFIG));
        Assert.assertEquals(DEFAULT_POLL_SIZE, configuration.getValues().get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
    }

    @Test
    public void consumeConfigurationWithCustomContainerPropertiesTest() {
        KafkaITContext context = getContextDouble();
        addDefaultConsumerProperties(context);

        final int maxFetchSize = 42;
        final int maxPollRecords = 43;

        context.withContainerProperty(Constants.KEY_MAX_FETCH_SIZE, String.valueOf(maxFetchSize)).withContainerProperty(
                Constants.KEY_MAX_POLL_RECORDS, String.valueOf(maxPollRecords));

        // build SUT
        ConsumerConfiguration configuration = createFromContextAndContainer(context);

        // asserts
        assertDefaultConsumerProperties(configuration);
        assertCredentials(configuration);
        assertProducerPropertiesNotPresent(configuration);

        // container properties
        Assert.assertEquals(maxFetchSize + MAX_REQUEST_SIZE_PADDING, configuration.getMaxRequestSize());
        Assert.assertEquals(maxPollRecords, configuration.getValues().get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
    }

    @Test(expected = ConnectorException.class)
    public void consumeConfigurationWithInvalidMaxPollRecordsTest() {
        KafkaITContext context = getContextDouble();
        addDefaultConsumerProperties(context);

        final int maxPollRecords = 0;
        context.withContainerProperty(Constants.KEY_MAX_POLL_RECORDS, String.valueOf(maxPollRecords));

        createFromContextAndContainer(context);
    }

    @Theory
    public void shouldFailWithBlankClientIDTest(@FromDataPoints("invalidClientIds") String clientId) {
        KafkaITContext context = getContextDouble();
        addDefaultConsumerProperties(context);
        context.addOperationProperty(Constants.KEY_CLIENT_ID, clientId);

        try {
            createFromContext(context);
        } catch (ConnectorException ignored) {
            return;
        }

        Assert.fail("should have thrown a ConnectorException");
    }

    @Test
    public void clientIDForTestConnectionTest() {
        KafkaITContext context = getContextDouble();
        addDefaultConsumerProperties(context);

        final String clientId = "client id from tests";
        context.addOperationProperty(Constants.KEY_CLIENT_ID, clientId);

        ConsumerConfiguration configuration = createFromContext(context);

        Assert.assertEquals(clientId, configuration.getClientId());
    }

    @Test
    public void commitOffsetConfigurationTest() {
        KafkaITContext context = getContextDouble();
        context.setOperationType(OperationType.EXECUTE);
        context.setOperationCustomType(CustomOperationType.COMMIT_OFFSET.name());

        context.addOperationProperty(Constants.KEY_CONSUMER_GROUP, CONSUMER_GROUP);
        context.addOperationProperty(Constants.KEY_CLIENT_ID, CLIENT_ID);

        // build SUT
        ConsumerConfiguration configuration = createFromContext(context);

        // asserts
        Assert.assertEquals(CLIENT_ID, configuration.getClientId());
        Map<String, ?> values = configuration.getValues();
        ConsumerConfig config = configuration.getConfig();

        Assert.assertEquals(CONSUMER_GROUP, values.get(ConsumerConfig.GROUP_ID_CONFIG));
        Assert.assertEquals(CONSUMER_GROUP, config.values().get(ConsumerConfig.GROUP_ID_CONFIG));

        // this properties are fixed and should always have the same values
        Assert.assertFalse("Autocommit should be disabled",
                ((Boolean) values.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)));
        Assert.assertFalse("Autocommit should be disabled",
                ((Boolean) config.values().get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)));

        assertCredentials(configuration);
        assertProducerPropertiesNotPresent(configuration);

        // container properties
        Assert.assertEquals(DEFAULT_MAX_FETCH_SIZE + MAX_REQUEST_SIZE_PADDING, configuration.getMaxRequestSize());
        Assert.assertEquals(DEFAULT_POLL_SIZE, configuration.getValues().get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
    }

    @Theory
    public void consumerGroupTest(@FromDataPoints("consumerGroups") String[] testCase) {
        String connectionGroup = testCase[0];
        String operationGroup = testCase[1];
        String expectedGroup = testCase[2];

        KafkaITContext context = getContextDouble();
        addDefaultConsumerProperties(context);

        context.addOperationProperty("consumer_group", operationGroup);
        context.addConnectionProperty("consumer_group", connectionGroup);

        ConsumerConfiguration configuration = createFromContext(context);

        Map<String, ?> values = configuration.getValues();
        ConsumerConfig config = configuration.getConfig();

        Assert.assertEquals(expectedGroup, values.get(ConsumerConfig.GROUP_ID_CONFIG));
        Assert.assertEquals(expectedGroup, config.values().get(ConsumerConfig.GROUP_ID_CONFIG));
    }

    @Test
    public void consumerGroupNotSetInOperationTest() {
        KafkaITContext context = getContextDouble();
        addDefaultConsumerProperties(context);

        context.addOperationProperty("consumer_group", StringUtil.EMPTY_STRING);
        context.addConnectionProperty("consumer_group", StringUtil.EMPTY_STRING);

        boolean exceptionThrown = false;
        try {
            createFromContext(context);
        } catch (ConnectorException ex) {
            exceptionThrown = true;
            Assert.assertEquals("Consumer Group cannot be blank", ex.getMessage());
        }

        Assert.assertTrue("an exception is expected", exceptionThrown);
    }

    @Test
    public void consumerGroupNotSetInBrowseTest() {
        KafkaITContext context = getContextDouble();

        context.addOperationProperty("consumer_group", StringUtil.EMPTY_STRING);
        context.addConnectionProperty("consumer_group", StringUtil.EMPTY_STRING);

        // for backwards compatibility, missing consumer group while running browse or test connection must not
        // produce any validation error
        ConsumerConfiguration.browse(new KafkaConnection<>(context));
    }

    private ConsumerConfiguration createFromContext(KafkaITContext context) {
        KafkaOperationConnection connection = TestUtils.createOperationConnection(context);
        return ConsumerConfiguration.consumer(connection);
    }

    private ConsumerConfiguration createFromContextAndContainer(KafkaITContext context) {
        KafkaOperationConnection connection = TestUtils.createOperationConnection(context);
        return ConsumerConfiguration.consumer(connection);
    }
}
