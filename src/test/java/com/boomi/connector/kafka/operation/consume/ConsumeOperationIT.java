
package com.boomi.connector.kafka.operation.consume;

import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.kafka.KafkaITContext;
import com.boomi.connector.kafka.TestUtils;
import com.boomi.connector.kafka.configuration.SASLMechanism;
import com.boomi.connector.kafka.operation.KafkaOperationConnection;
import com.boomi.connector.kafka.util.Constants;
import com.boomi.connector.testutil.ResponseFactory;
import com.boomi.connector.testutil.SimpleOperationResponse;
import com.boomi.connector.testutil.SimpleOperationResult;
import com.boomi.connector.testutil.SimplePayloadMetadata;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.boomi.util.ClassUtil;
import com.boomi.util.CollectionUtil;
import com.boomi.util.IOUtil;
import com.boomi.util.StreamUtil;
import com.boomi.util.StringUtil;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(Theories.class)
public class ConsumeOperationIT {

    private static final String KEY_PREFIX = "Key for Consume IT ";
    private static final String PAYLOAD_PREFIX = "Message Payload for Consume IT ";

    private static final String CLIENT_ID = "Consume Operation IT";
    private static final long DEFAULT_TIMEOUT = 30_000L; // 30 seconds
    private static final String EXPECTED_PARTITION = "0";

    private static final int POSTED_MESSAGES = 300;
    private static final int NO_OFFSET = 0;

    private static final Collection<String> TEST_TOPICS = new ArrayList<>();

    private static String _topic;
    private static long _runtimeTimestamp;

    static {
        TestUtils.disableLogs();
    }

    @BeforeClass
    public static void createTopic() {
        _runtimeTimestamp = System.currentTimeMillis();
        _topic = "ConsumeOperationIT-" + System.currentTimeMillis();
        TEST_TOPICS.add(_topic);

        KafkaITContext context = KafkaITContext.adminVMClient().setProduce();
        TestUtils.createTopic(context, _topic);

        Collection<ProducerRecord<String, String>> records = new ArrayList<>(POSTED_MESSAGES);
        for (int index = 0; index < POSTED_MESSAGES; index++) {
            records.add(new ProducerRecord<>(_topic, key(index), payload(index)));
        }

        TestUtils.publishMessages(context, records);
    }

    @AfterClass
    public static void deleteTopic() {
        KafkaITContext context = KafkaITContext.adminVMClient().setConsume();
        context.addOperationProperty(Constants.KEY_MIN_MESSAGES, 1L);
        context.addOperationProperty(Constants.KEY_RECEIVE_MESSAGE_TIMEOUT, 1000L);
        TestUtils.deleteTopic(context, TEST_TOPICS);
    }

    private static String key(long index) {
        return KEY_PREFIX + index;
    }

    private static String payload(long index) {
        return PAYLOAD_PREFIX + index;
    }

    private static void configureOperation(KafkaITContext context, long minMessages, String group, boolean autocommit) {
        context.addOperationProperty(Constants.KEY_CONSUMER_GROUP, group);
        context.addOperationProperty(Constants.KEY_CLIENT_ID, CLIENT_ID);
        context.addOperationProperty(Constants.KEY_RECEIVE_MESSAGE_TIMEOUT, DEFAULT_TIMEOUT);
        context.addOperationProperty(Constants.KEY_MIN_MESSAGES, minMessages);
        context.addOperationProperty(Constants.KEY_AUTOCOMMIT, autocommit);
    }

    private static void assertMessages(long expectedMessages, long indexOffset, List<byte[]> payloads,
            List<SimplePayloadMetadata> metadatas) {
        for (int index = 0; index < expectedMessages; index++) {
            SimplePayloadMetadata metadata = metadatas.get(index);
            Map<String, String> props = metadata.getTrackedProps();
            byte[] payload = payloads.get(index);

            long adjustedIndex = index + indexOffset;

            assertMessage(adjustedIndex, props, payload);
        }
    }

    private static void assertMessage(long index, Map<String, String> props, byte[] payload) {
        assertEquals("Tracked Property: Key", key(index), props.get(Constants.KEY_MESSAGE_KEY));
        assertEquals("Tracked Property: Topic Name", _topic, props.get(Constants.KEY_TOPIC_NAME));
        assertEquals("Tracked Property: Partition", EXPECTED_PARTITION, props.get(Constants.KEY_TOPIC_PARTITION));
        assertEquals("Tracked Property: Offset", String.valueOf(index), props.get(Constants.KEY_MESSAGE_OFFSET));

        long messageTimestamp = Long.parseLong(props.get(Constants.KEY_MESSAGE_TIMESTAMP));
        assertTrue("Tracked Property: Message Timestamp",
                (messageTimestamp > _runtimeTimestamp) && (messageTimestamp < System.currentTimeMillis()));

        Assert.assertNotNull(payload);
        assertEquals("Message Payload", payload(index), new String(payload));
    }

    private static void assertPayloadsAndMetadatasSize(long expectedMessages, List<byte[]> payloads,
            List<SimplePayloadMetadata> metadatas) {
        Assert.assertEquals("amount of payloads and metadatas should be equal", payloads.size(), metadatas.size());
        Assert.assertEquals(expectedMessages + " expected messages", expectedMessages, payloads.size());
    }

    private static String bigPayload() throws IOException {
        InputStream resource = null;
        try {
            resource = ClassUtil.getResourceAsStream("12MB.txt");
            return StreamUtil.toString(resource, StringUtil.UTF8_CHARSET);
        } finally {
            IOUtil.closeQuietly(resource);
        }
    }

    private static SimpleTrackedData createEmptyData() {
        return new SimpleTrackedData(0, null);
    }

    @DataPoints("invalid authentication contexts")
    public static Collection<ContextAndErrorMessage> invalidAuthenticationContexts() {
        List<ContextAndErrorMessage> contextsAndErrors = new ArrayList<>();

        // Invalid Username & Password for valid Protocol/Mechanism
        KafkaITContext invalidUsernameAndPassword = KafkaITContext.consumeVM();
        // set host with enabled authentication
        invalidUsernameAndPassword.addConnectionProperty(Constants.KEY_SERVERS,
                KafkaITContext.VM_WITH_SHA256_PLAINTEXT);
        // set SASL and SCRAM SHA 256
        invalidUsernameAndPassword.addConnectionProperty(Constants.KEY_SECURITY_PROTOCOL,
                SecurityProtocol.SASL_PLAINTEXT.name);
        invalidUsernameAndPassword.addConnectionProperty(Constants.KEY_SASL_MECHANISM,
                SASLMechanism.SCRAM_SHA_256.getMechanism());
        // wrong authentication credentials
        invalidUsernameAndPassword.addConnectionProperty(Constants.KEY_USERNAME, "wrong username");
        invalidUsernameAndPassword.addConnectionProperty(Constants.KEY_PASSWORD, "wrong password");

        contextsAndErrors.add(new ContextAndErrorMessage(invalidUsernameAndPassword, "Authentication failed"));

        // Invalid Username & Password for valid Protocol/Mechanism
        KafkaITContext invalidSaslMechanism = KafkaITContext.consumeVM();
        // set SASL and SCRAM SHA 256 though is not supported by the server
        invalidSaslMechanism.addConnectionProperty(Constants.KEY_SECURITY_PROTOCOL,
                SecurityProtocol.SASL_PLAINTEXT.name);
        invalidSaslMechanism.addConnectionProperty(Constants.KEY_SASL_MECHANISM,
                SASLMechanism.SCRAM_SHA_256.getMechanism());
        // wrong authentication credentials
        invalidSaslMechanism.addConnectionProperty(Constants.KEY_USERNAME, "some user");
        invalidSaslMechanism.addConnectionProperty(Constants.KEY_PASSWORD, "some pass");

        contextsAndErrors.add(new ContextAndErrorMessage(invalidSaslMechanism,
                "Unexpected handshake request with client mechanism SCRAM-SHA-256"));

        // Empty broker list
        KafkaITContext emptyBrokerList = KafkaITContext.consumeVM();
        emptyBrokerList.addConnectionProperty(Constants.KEY_SERVERS, StringUtil.EMPTY_STRING);

        contextsAndErrors.add(new ContextAndErrorMessage(emptyBrokerList, "no resolvable bootstrap urls given"));

        return contextsAndErrors;
    }

    @Test
    public void consumerTest() {
        KafkaITContext context = KafkaITContext.consumeVM();
        context.setObjectTypeId(_topic);

        final long expectedMessages = 5L;
        String consumerGroup = "consumerTest" + System.currentTimeMillis();
        configureOperation(context, expectedMessages, consumerGroup, true);

        KafkaOperationConnection connection = TestUtils.createOperationConnection(context);
        ConsumeOperation operation = new ConsumeOperation(connection);

        SimpleTrackedData data = createEmptyData();

        SimpleOperationResponse response = ResponseFactory.get(data);
        UpdateRequest request = TestUtils.stubRequest(data);

        // execute SUT
        operation.executeUpdate(request, response);

        // Asserts
        SimpleOperationResult result = CollectionUtil.getFirst(response.getResults());

        List<byte[]> payloads = result.getPayloads();
        List<SimplePayloadMetadata> metadatas = result.getPayloadMetadatas();

        assertPayloadsAndMetadatasSize(expectedMessages, payloads, metadatas);

        assertMessages(expectedMessages, NO_OFFSET, payloads, metadatas);
    }

    @Test
    public void overrideTopicNameWithDynamicTopicFieldTest() {
        KafkaITContext context = KafkaITContext.consumeVM();
        context.setObjectTypeId(Constants.DYNAMIC_TOPIC_ID);
        context.addOperationProperty(Constants.TOPIC_NAME_FIELD_ID, _topic);

        final long expectedMessages = 5L;
        String consumerGroup = "consumerTest" + System.currentTimeMillis();
        configureOperation(context, expectedMessages, consumerGroup, true);

        KafkaOperationConnection connection = TestUtils.createOperationConnection(context);
        ConsumeOperation operation = new ConsumeOperation(connection);

        SimpleTrackedData data = createEmptyData();

        SimpleOperationResponse response = ResponseFactory.get(data);
        UpdateRequest request = TestUtils.stubRequest(data);

        // execute SUT
        operation.executeUpdate(request, response);

        // Asserts
        SimpleOperationResult result = CollectionUtil.getFirst(response.getResults());

        List<byte[]> payloads = result.getPayloads();
        List<SimplePayloadMetadata> metadatas = result.getPayloadMetadatas();

        assertPayloadsAndMetadatasSize(expectedMessages, payloads, metadatas);
        assertEquals(OperationStatus.SUCCESS, result.getStatus());
        assertMessages(expectedMessages, NO_OFFSET, payloads, metadatas);
    }

    @Test
    public void overrideTopicNameWithDynamicTopicFieldEmptyTest() {
        KafkaITContext context = KafkaITContext.consumeVM();
        context.setObjectTypeId(Constants.DYNAMIC_TOPIC_ID);
        context.addOperationProperty(Constants.TOPIC_NAME_FIELD_ID, "");

        final long expectedMessages = 5L;
        String consumerGroup = "consumerTest" + System.currentTimeMillis();
        configureOperation(context, expectedMessages, consumerGroup, true);

        KafkaOperationConnection connection = TestUtils.createOperationConnection(context);
        ConsumeOperation operation = new ConsumeOperation(connection);

        SimpleTrackedData data = createEmptyData();

        SimpleOperationResponse response = ResponseFactory.get(data);
        UpdateRequest request = TestUtils.stubRequest(data);

        // execute SUT
        operation.executeUpdate(request, response);

        // Asserts
        SimpleOperationResult result = CollectionUtil.getFirst(response.getResults());

        List<byte[]> payloads = result.getPayloads();
        List<SimplePayloadMetadata> metadatas = result.getPayloadMetadatas();

        assertEquals(0, payloads.size());
        assertEquals(0, metadatas.size());
        assertEquals(OperationStatus.FAILURE, result.getStatus());
        assertEquals(Constants.CODE_ERROR, result.getStatusCode());

    }

    @Test
    public void autocommitEnableTest() {
        KafkaITContext context = KafkaITContext.consumeVM();
        context.setObjectTypeId(_topic);

        final long expectedMessages = 7L;
        String consumerGroup = "autocommitEnableTest" + System.currentTimeMillis();

        configureOperation(context, expectedMessages, consumerGroup, true);

        KafkaOperationConnection connection = TestUtils.createOperationConnection(context);
        ConsumeOperation operation = new ConsumeOperation(connection);

        // first run
        SimpleTrackedData firstData = createEmptyData();
        SimpleOperationResponse firstResponse = ResponseFactory.get(firstData);
        UpdateRequest firstRequest = TestUtils.stubRequest(firstData);
        operation.executeUpdate(firstRequest, firstResponse);

        // second run
        SimpleTrackedData secondData = createEmptyData();
        SimpleOperationResponse secondResponse = ResponseFactory.get(secondData);
        UpdateRequest secondRequest = TestUtils.stubRequest(secondData);
        operation.executeUpdate(secondRequest, secondResponse);

        // asserts
        SimpleOperationResult firstResult = CollectionUtil.getFirst(firstResponse.getResults());
        List<byte[]> firstPayloads = firstResult.getPayloads();
        List<SimplePayloadMetadata> firstMetadatas = firstResult.getPayloadMetadatas();

        assertPayloadsAndMetadatasSize(expectedMessages, firstPayloads, firstMetadatas);

        SimpleOperationResult secondResult = CollectionUtil.getFirst(secondResponse.getResults());
        List<byte[]> secondPayloads = secondResult.getPayloads();
        List<SimplePayloadMetadata> secondMetadatas = secondResult.getPayloadMetadatas();

        assertPayloadsAndMetadatasSize(expectedMessages, secondPayloads, secondMetadatas);

        // assert first execution
        assertMessages(expectedMessages, NO_OFFSET, firstPayloads, firstMetadatas);
        // assert second execution
        assertMessages(expectedMessages, expectedMessages, secondPayloads, secondMetadatas);
    }

    @Test
    public void autocommitDisableTest() {
        KafkaITContext context = KafkaITContext.consumeVM();
        context.setObjectTypeId(_topic);

        final long expectedMessages = 7L;
        String consumerGroup = "autocommitDisableTest" + System.currentTimeMillis();

        configureOperation(context, expectedMessages, consumerGroup, false);

        KafkaOperationConnection connection = TestUtils.createOperationConnection(context);
        ConsumeOperation operation = new ConsumeOperation(connection);

        // first run
        SimpleTrackedData firstData = createEmptyData();
        SimpleOperationResponse firstResponse = ResponseFactory.get(firstData);
        UpdateRequest firstRequest = TestUtils.stubRequest(firstData);
        operation.executeUpdate(firstRequest, firstResponse);

        // second run
        SimpleTrackedData secondData = createEmptyData();
        SimpleOperationResponse secondResponse = ResponseFactory.get(secondData);
        UpdateRequest secondRequest = TestUtils.stubRequest(secondData);
        operation.executeUpdate(secondRequest, secondResponse);

        // asserts
        SimpleOperationResult firstResult = CollectionUtil.getFirst(firstResponse.getResults());
        List<byte[]> firstPayloads = firstResult.getPayloads();
        List<SimplePayloadMetadata> firstMetadatas = firstResult.getPayloadMetadatas();

        assertPayloadsAndMetadatasSize(expectedMessages, firstPayloads, firstMetadatas);

        SimpleOperationResult secondResult = CollectionUtil.getFirst(secondResponse.getResults());
        List<byte[]> secondPayloads = secondResult.getPayloads();
        List<SimplePayloadMetadata> secondMetadatas = secondResult.getPayloadMetadatas();

        assertPayloadsAndMetadatasSize(expectedMessages, secondPayloads, secondMetadatas);

        // assert first execution
        assertMessages(expectedMessages, NO_OFFSET, firstPayloads, firstMetadatas);
        // assert second execution
        assertMessages(expectedMessages, NO_OFFSET, secondPayloads, secondMetadatas);
    }

    //@Test
    public void largeMessageInBetweenWithoutAutocommitTest() {
        // prepare topic for the test
        final String topic = "largeMessageInBetweenWithoutAutocommitTest-" + System.currentTimeMillis();
        TEST_TOPICS.add(topic);
        final int postedMessages = 4;
        KafkaITContext adminContext = KafkaITContext.adminVMClient().setProduce();

        try {
            TestUtils.createTopic(adminContext, topic);
        } catch (Exception e) {
            Assert.fail("failed creating the topic: " + e.getMessage());
        }
        try {
            Collection<ProducerRecord<String, String>> records = new ArrayList<>(postedMessages);
            records.add(new ProducerRecord<>(topic, key(0), payload(0)));
            records.add(new ProducerRecord<>(topic, key(1), payload(1)));
            records.add(new ProducerRecord<>(topic, key(2), bigPayload()));
            records.add(new ProducerRecord<>(topic, key(3), payload(3)));

            TestUtils.publishMessages(adminContext, records);
        } catch (Exception e) {
            Assert.fail("failed posting the messages: " + e.getMessage());
        }

        // prepare operation
        KafkaITContext context = KafkaITContext.consumeVM();
        context.setObjectTypeId(topic);

        String consumerGroup = "largeMessageInBetweenWithoutAutocommitTest" + System.currentTimeMillis();

        configureOperation(context, 4L, consumerGroup, false);

        KafkaOperationConnection connection = TestUtils.createOperationConnection(context);
        ConsumeOperation operation = new ConsumeOperation(connection);

        SimpleTrackedData data = createEmptyData();
        SimpleOperationResponse response = ResponseFactory.get(data);
        UpdateRequest request = TestUtils.stubRequest(data);

        // execute operation
        operation.executeUpdate(request, response);

        // asserts
        Assert.assertEquals(1, response.getResults().size());
        SimpleOperationResult result = CollectionUtil.getFirst(response.getResults());

        List<byte[]> payloads = result.getPayloads();
        List<SimplePayloadMetadata> payloadMetadatas = result.getPayloadMetadatas();

        final int expectedMessages = 3;

        assertEquals(expectedMessages, payloads.size());
        assertEquals(expectedMessages, payloadMetadatas.size());

        final int expectedSuccessMessages = 2;

        for (int index = 0; index < expectedSuccessMessages; index++) {
            byte[] payload = payloads.get(index);
            SimplePayloadMetadata metadata = payloadMetadatas.get(index);
            Map<String, String> props = metadata.getTrackedProps();

            assertEquals(payload(index), new String(payload));
            assertEquals(key(index), props.get(Constants.KEY_MESSAGE_KEY));
            assertEquals(topic, props.get(Constants.KEY_TOPIC_NAME));
            assertEquals(String.valueOf(index), props.get(Constants.KEY_MESSAGE_OFFSET));
            assertNotNull(props.get(Constants.KEY_TOPIC_PARTITION));
        }

        byte[] errorPayload = payloads.get(2);
        SimplePayloadMetadata errorMetadata = payloadMetadatas.get(2);
        Map<String, String> props = errorMetadata.getTrackedProps();

        assertEquals(StringUtil.EMPTY_STRING, new String(errorPayload));
        assertEquals(topic, props.get(Constants.KEY_TOPIC_NAME));
        assertEquals(String.valueOf(2), props.get(Constants.KEY_MESSAGE_OFFSET));
        assertNotNull(props.get(Constants.KEY_TOPIC_PARTITION));
    }

    //@Test
    public void largeMessageInBetweenWithAutocommitTest() {
        // prepare topic for the test
        final String topic = "largeMessageInBetweenWithAutocommitTest-" + System.currentTimeMillis();
        TEST_TOPICS.add(topic);
        final int postedMessages = 5;
        KafkaITContext adminContext = KafkaITContext.adminVMClient().setProduce();

        try {
            TestUtils.createTopic(adminContext, topic);
        } catch (Exception e) {
            Assert.fail("failed creating the topic: " + e.getMessage());
        }
        try {
            Collection<ProducerRecord<String, String>> records = new ArrayList<>(postedMessages);
            records.add(new ProducerRecord<>(topic, key(0), payload(0)));
            records.add(new ProducerRecord<>(topic, key(1), payload(1)));
            records.add(new ProducerRecord<>(topic, key(2), bigPayload()));
            records.add(new ProducerRecord<>(topic, key(3), payload(3)));
            records.add(new ProducerRecord<>(topic, key(4), payload(4)));

            TestUtils.publishMessages(adminContext, records);
        } catch (Exception e) {
            Assert.fail("failed posting the messages: " + e.getMessage());
        }

        // prepare operation
        KafkaITContext context = KafkaITContext.consumeVM();
        context.setObjectTypeId(topic);

        String consumerGroup = "largeMessageInBetweenWithAutocommitTest" + System.currentTimeMillis();

        configureOperation(context, postedMessages, consumerGroup, true);

        KafkaOperationConnection connection = TestUtils.createOperationConnection(context);
        ConsumeOperation operation = new ConsumeOperation(connection);

        SimpleTrackedData data = createEmptyData();
        SimpleOperationResponse response = ResponseFactory.get(data);
        UpdateRequest request = TestUtils.stubRequest(data);

        // execute operation
        operation.executeUpdate(request, response);

        // asserts
        Assert.assertEquals(1, response.getResults().size());
        SimpleOperationResult result = CollectionUtil.getFirst(response.getResults());

        List<byte[]> payloads = result.getPayloads();
        List<SimplePayloadMetadata> payloadMetadatas = result.getPayloadMetadatas();
        assertEquals(postedMessages, payloads.size());
        assertEquals(postedMessages, payloadMetadatas.size());

        for (int index = 0; index < postedMessages; index++) {
            byte[] payload = payloads.get(index);
            SimplePayloadMetadata metadata = payloadMetadatas.get(index);
            Map<String, String> props = metadata.getTrackedProps();

            // error message
            if (index == 2) {
                assertEquals(StringUtil.EMPTY_STRING, new String(payload));
            } else {
                assertEquals(payload(index), new String(payload));
                assertEquals(key(index), props.get(Constants.KEY_MESSAGE_KEY));
            }

            assertEquals(topic, props.get(Constants.KEY_TOPIC_NAME));
            assertEquals(String.valueOf(index), props.get(Constants.KEY_MESSAGE_OFFSET));
            assertNotNull(props.get(Constants.KEY_TOPIC_PARTITION));
        }
    }

    @Theory
    public void invalidCredentialsTest(
            @FromDataPoints("invalid authentication contexts") ContextAndErrorMessage contextsAndErrors) {
        contextsAndErrors._context.setObjectTypeId(_topic);

        final long expectedMessages = 1L;
        String consumerGroup = "invalidCredentialsTest" + System.currentTimeMillis();
        configureOperation(contextsAndErrors._context, expectedMessages, consumerGroup, true);

        KafkaOperationConnection connection = TestUtils.createOperationConnection(contextsAndErrors._context);
        ConsumeOperation operation = new ConsumeOperation(connection);

        SimpleTrackedData data = createEmptyData();

        SimpleOperationResponse response = ResponseFactory.get(data);
        UpdateRequest request = TestUtils.stubRequest(data);

        // execute SUT
        operation.executeUpdate(request, response);

        // Asserts
        SimpleOperationResult result = CollectionUtil.getFirst(response.getResults());

        List<byte[]> payloads = result.getPayloads();
        List<SimplePayloadMetadata> metadatas = result.getPayloadMetadatas();

        assertEquals(0, payloads.size());
        assertEquals(0, metadatas.size());

        assertEquals(OperationStatus.FAILURE, result.getStatus());
        assertEquals(Constants.CODE_ERROR, result.getStatusCode());
        assertTrue("unexpected error message",
                StringUtil.containsIgnoreCase(result.getMessage(), contextsAndErrors._errorMessage));
    }

    private static class ContextAndErrorMessage {

        private final KafkaITContext _context;
        private final String _errorMessage;

        ContextAndErrorMessage(KafkaITContext context, String errorMessage) {
            _context = context;
            _errorMessage = errorMessage;
        }
    }
}