
package com.boomi.connector.kafka.operation.commit;

import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.kafka.KafkaITContext;
import com.boomi.connector.kafka.TestUtils;
import com.boomi.connector.kafka.util.Constants;
import com.boomi.connector.testutil.ResponseFactory;
import com.boomi.connector.testutil.SimpleOperationResponse;
import com.boomi.connector.testutil.SimpleOperationResult;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.boomi.util.CollectionUtil;
import com.boomi.util.StringUtil;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class CommitOffsetOperationIT {

    private static final String KEY_PREFIX = "Key for Commit Offset Operation IT ";
    private static final String PAYLOAD_PREFIX = "Message Payload for Commit Offset Operation IT ";
    private static final int POSTED_MESSAGES = 100;

    private static String _topic;

    static {
        TestUtils.disableLogs();
    }

    @BeforeClass
    public static void createTopic() {
        _topic = "CommitOffsetOperationIT-" + System.currentTimeMillis();

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
        KafkaITContext context = KafkaITContext.adminVMClient().setCommit();
        TestUtils.deleteTopic(context, Collections.singleton(_topic));
    }

    private static String key(long index) {
        return KEY_PREFIX + index;
    }

    private static String payload(long index) {
        return PAYLOAD_PREFIX + index;
    }

    @Test
    public void commitOffsetOperationTest() {
        final String consumerGroup = "commitOffsetOperationTest-" + System.currentTimeMillis();

        final int partition = 0;
        final long offset = 10L;
        final String metadata = "some metadata to send with the commit";

        KafkaITContext context = KafkaITContext.commitOffsetVM();
        context.setObjectTypeId(_topic);
        context.addOperationProperty(Constants.KEY_CONSUMER_GROUP, consumerGroup);

        SimpleTrackedData data1 = TestUtils.createCommitDocument(0, partition, offset - 2L, metadata);
        SimpleTrackedData data2 = TestUtils.createCommitDocument(1, partition, offset - 5L, metadata);
        SimpleTrackedData data3 = TestUtils.createCommitDocument(2, partition, offset, metadata);
        SimpleTrackedData data4 = TestUtils.createCommitDocument(3, partition, offset - 1L, metadata);
        SimpleOperationResponse response = ResponseFactory.get(data1, data2, data3, data4);
        UpdateRequest request = TestUtils.stubRequest(data1, data2, data3, data4);

        CommitOffsetOperation operation = new CommitOffsetOperation(TestUtils.createOperationConnection(context));

        // execute sut
        operation.executeUpdate(request, response);

        // asserts
        List<SimpleOperationResult> results = response.getResults();

        Assert.assertEquals(4, results.size());
        for (SimpleOperationResult result : results) {
            Assert.assertEquals(OperationStatus.SUCCESS, result.getStatus());
        }

        assertNextMessage(consumerGroup, offset + 1);
    }

    @Test
    public void commitOffsetOperationInvalidJSONTest() {
        final String consumerGroup = "commitOffsetOperationInvalidJSONTest-" + System.currentTimeMillis();

        KafkaITContext context = KafkaITContext.commitOffsetVM();
        context.setObjectTypeId(_topic);
        context.addOperationProperty(Constants.KEY_CONSUMER_GROUP, consumerGroup);

        SimpleTrackedData data = new SimpleTrackedData(0,
                new ByteArrayInputStream("invalid json :(".getBytes(StringUtil.UTF8_CHARSET)));
        SimpleOperationResponse response = ResponseFactory.get(data);
        UpdateRequest request = TestUtils.stubRequest(data);

        CommitOffsetOperation operation = new CommitOffsetOperation(TestUtils.createOperationConnection(context));

        // execute sut
        operation.executeUpdate(request, response);

        // asserts
        List<SimpleOperationResult> results = response.getResults();

        Assert.assertEquals(1, results.size());
        for (SimpleOperationResult result : results) {
            Assert.assertEquals(OperationStatus.APPLICATION_ERROR, result.getStatus());
        }

        assertNoMessageCommitted(consumerGroup);
    }

    @Test
    public void commitOffsetOperationWrongTopicTest() {
        final String consumerGroup = "commitOffsetOperationWrongTopicTest-" + System.currentTimeMillis();
        final int partition = 0;
        final long offset = 10L;
        final String metadata = "some metadata to send with the commit";

        KafkaITContext context = KafkaITContext.commitOffsetVM();
        context.setObjectTypeId("wrong topic");
        context.addOperationProperty(Constants.KEY_CONSUMER_GROUP, consumerGroup);

        SimpleTrackedData data = TestUtils.createCommitDocument(0, partition, offset, metadata);
        SimpleOperationResponse response = ResponseFactory.get(data);
        UpdateRequest request = TestUtils.stubRequest(data);

        CommitOffsetOperation operation = new CommitOffsetOperation(TestUtils.createOperationConnection(context));

        // execute sut
        operation.executeUpdate(request, response);

        // asserts
        List<SimpleOperationResult> results = response.getResults();

        Assert.assertEquals(1, results.size());
        for (SimpleOperationResult result : results) {
            Assert.assertEquals(OperationStatus.APPLICATION_ERROR, result.getStatus());
        }

        assertNoMessageCommitted(consumerGroup);
    }

    @Test
    public void overrideTopicNameFromDynamicTopicFieldTest() {
        final String consumerGroup = "commitOffsetOperationTest-" + System.currentTimeMillis();

        final int partition = 0;
        final long offset = 10L;
        final String metadata = "some metadata to send with the commit";

        KafkaITContext context = KafkaITContext.commitOffsetVM();
        context.setObjectTypeId(Constants.DYNAMIC_TOPIC_ID);
        context.addOperationProperty(Constants.KEY_CONSUMER_GROUP, consumerGroup);
        context.addOperationProperty(Constants.TOPIC_NAME_FIELD_ID, _topic);

        SimpleTrackedData data1 = TestUtils.createCommitDocument(0, partition, offset - 2L, metadata);
        SimpleTrackedData data2 = TestUtils.createCommitDocument(1, partition, offset - 5L, metadata);
        SimpleTrackedData data3 = TestUtils.createCommitDocument(2, partition, offset, metadata);
        SimpleTrackedData data4 = TestUtils.createCommitDocument(3, partition, offset - 1L, metadata);
        SimpleOperationResponse response = ResponseFactory.get(data1, data2, data3, data4);
        UpdateRequest request = TestUtils.stubRequest(data1, data2, data3, data4);

        CommitOffsetOperation operation = new CommitOffsetOperation(TestUtils.createOperationConnection(context));

        // execute sut
        operation.executeUpdate(request, response);

        // asserts
        List<SimpleOperationResult> results = response.getResults();

        Assert.assertEquals(4, results.size());
        for (SimpleOperationResult result : results) {
            Assert.assertEquals(OperationStatus.SUCCESS, result.getStatus());
        }

        assertNextMessage(consumerGroup, offset + 1);
    }


    @Test
    public void overrideTopicNameFromDynamicTopicFieldEmptyTest() {
        final String consumerGroup = "commitOffsetOperationTest-" + System.currentTimeMillis();

        final int partition = 0;
        final long offset = 10L;
        final String metadata = "some metadata to send with the commit";

        KafkaITContext context = KafkaITContext.commitOffsetVM();
        context.setObjectTypeId(Constants.DYNAMIC_TOPIC_ID);
        context.addOperationProperty(Constants.KEY_CONSUMER_GROUP, consumerGroup);
        context.addOperationProperty(Constants.TOPIC_NAME_FIELD_ID, "");

        SimpleTrackedData data1 = TestUtils.createCommitDocument(0, partition, offset - 2L, metadata);
        SimpleTrackedData data2 = TestUtils.createCommitDocument(1, partition, offset - 5L, metadata);
        SimpleTrackedData data3 = TestUtils.createCommitDocument(2, partition, offset, metadata);
        SimpleTrackedData data4 = TestUtils.createCommitDocument(3, partition, offset - 1L, metadata);
        SimpleOperationResponse response = ResponseFactory.get(data1, data2, data3, data4);
        UpdateRequest request = TestUtils.stubRequest(data1, data2, data3, data4);

        CommitOffsetOperation operation = new CommitOffsetOperation(TestUtils.createOperationConnection(context));

        // execute sut
        operation.executeUpdate(request, response);

        // asserts
        List<SimpleOperationResult> results = response.getResults();

        Assert.assertEquals(4, results.size());
        for (SimpleOperationResult result : results) {
            Assert.assertEquals(OperationStatus.APPLICATION_ERROR, result.getStatus());
        }

        assertNoMessageCommitted(consumerGroup);
    }

    private void assertNextMessage(String consumerGroup, long offset) {
        KafkaITContext consumerContext = KafkaITContext.consumeVM();
        consumerContext.addOperationProperty(Constants.KEY_CONSUMER_GROUP, consumerGroup);
        Collection<ConsumerRecord<String, InputStream>> records = TestUtils.consumeMessages(consumerContext, _topic);

        ConsumerRecord<String, InputStream> record = CollectionUtil.getFirst(records);

        Assert.assertEquals(offset, record.offset());
    }

    private void assertNoMessageCommitted(String consumerGroup) {
        assertNextMessage(consumerGroup, 0L);
    }
}
