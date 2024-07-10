
package com.boomi.connector.kafka.operation.produce;

import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.kafka.KafkaITContext;
import com.boomi.connector.kafka.TestUtils;
import com.boomi.connector.kafka.operation.KafkaOperationConnection;
import com.boomi.connector.kafka.util.Constants;
import com.boomi.connector.testutil.ResponseFactory;
import com.boomi.connector.testutil.SimpleOperationResponse;
import com.boomi.connector.testutil.SimpleOperationResult;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.boomi.util.ByteUnit;
import com.boomi.util.CollectionUtil;
import com.boomi.util.StringUtil;
import com.boomi.util.io.FastByteArrayInputStream;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.InputStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@RunWith(Theories.class)
public class ProduceOperationIT {

    @DataPoints("compression")
    public static final String[] COMPRESSION_TYPES = new String[] { "snappy", "none", "lz4", "gzip" };

    @DataPoints("ack")
    public static final String[] ACKS = new String[] { "0", "1", "all" };

    private static final String PAYLOAD = "{0} - {1}";

    private static String _topic = null;

    static {
        TestUtils.disableLogs();
    }

    private static SimpleTrackedData buildData(int index, String payload, String key, String topic,
            Map<String, String> headers) {
        InputStream data = new FastByteArrayInputStream(payload.getBytes(StringUtil.UTF8_CHARSET));

        return new SimpleTrackedData(index, data, headers, CollectionUtil.<String, String>mapBuilder().put(
                Constants.KEY_MESSAGE_KEY, key).put(Constants.KEY_TOPIC_NAME, topic).finishImmutable());
    }

    private static SimpleTrackedData buildData(int index, String payload, String key, String topic) {
        return buildData(index, payload, key, topic, Collections.<String, String>emptyMap());
    }

    private static SimpleTrackedData buildData(int index, String payload, String key) {
        return buildData(index, payload, key, null, Collections.<String, String>emptyMap());
    }

    @BeforeClass
    public static void createTopic() {
        _topic = "ProduceOperationIT-" + System.currentTimeMillis();
        TestUtils.createTopic(KafkaITContext.adminVMClient().setProduce(), _topic);
    }

    @AfterClass
    public static void deleteTopic() {
        TestUtils.deleteTopic(KafkaITContext.adminVMClient().setProduce(), Collections.singleton(_topic));
        _topic = null;
    }

    @DataPoints(value = "invalid object data")
    public static Collection<SimpleTrackedData> invalidObjectDatas() {
        Collection<SimpleTrackedData> datas = new ArrayList<>();

        SimpleTrackedData dataThatThrowsIOExceptionOnRead = new SimpleTrackedData(0, new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException("error reading stream");
            }
        });
        datas.add(dataThatThrowsIOExceptionOnRead);

        SimpleTrackedData dataThatIs1GB = new SimpleTrackedData(0, null) {
            @Override
            public long getDataSize() {
                return ByteUnit.byteSize(1, ByteUnit.GB.name());
            }
        };
        datas.add(dataThatIs1GB);

        SimpleTrackedData dataThatThrowsIOExceptionOnSize = new SimpleTrackedData(0, null) {
            @Override
            public long getDataSize() throws IOException {
                throw new IOException();
            }
        };
        datas.add(dataThatThrowsIOExceptionOnSize);

        return datas;
    }

    @Theory
    public void produceTest(@FromDataPoints("compression") String compression, @FromDataPoints("ack") String ack) {
        // the version installed in the VM does not support snappy
        Assume.assumeFalse("snappy".equals(compression));

        KafkaITContext context = KafkaITContext.produceVM();
        context.setObjectTypeId(_topic);

        context.addOperationProperty(Constants.KEY_ACKS, ack);
        context.addOperationProperty(Constants.KEY_COMPRESSION_TYPE, compression);

        KafkaOperationConnection connection = TestUtils.createOperationConnection(context);

        ProduceOperation operation = new ProduceOperation(connection);

        SimpleTrackedData[] datas = new SimpleTrackedData[] {
                buildData(0, MessageFormat
                                .format(PAYLOAD, "this is message 1 " + compression + " " + ack,
                                        System.currentTimeMillis()),
                        "key1"), buildData(1, MessageFormat
                .format(PAYLOAD, "this is message 2 " + compression + " " + ack, System.currentTimeMillis()), "key2"),
                buildData(2, MessageFormat
                                .format(PAYLOAD, "this is message 3 " + compression + " " + ack,
                                        System.currentTimeMillis()),
                        "key3") };

        SimpleOperationResponse response = ResponseFactory.get(datas);
        UpdateRequest request = TestUtils.stubRequest(datas);

        // execute SUT
        operation.executeUpdate(request, response);

        List<SimpleOperationResult> results = response.getResults();

        Assert.assertEquals(datas.length, results.size());

        for (SimpleOperationResult result : results) {
            Assert.assertEquals(OperationStatus.SUCCESS, result.getStatus());
        }
    }

    @Theory
    public void invalidObjectDataTest(@FromDataPoints("invalid object data") SimpleTrackedData data) {
        KafkaITContext context = KafkaITContext.produceVM();
        context.setObjectTypeId(_topic);

        KafkaOperationConnection connection = TestUtils.createOperationConnection(context);

        ProduceOperation operation = new ProduceOperation(connection);

        SimpleOperationResponse response = ResponseFactory.get(data);
        UpdateRequest request = TestUtils.stubRequest(data);

        // execute SUT
        operation.executeUpdate(request, response);

        List<SimpleOperationResult> results = response.getResults();

        Assert.assertEquals(1, results.size());
        SimpleOperationResult result = CollectionUtil.getFirst(results);
        Assert.assertEquals(OperationStatus.APPLICATION_ERROR, result.getStatus());
        Assert.assertEquals(Constants.CODE_INVALID_SIZE, result.getStatusCode());
    }

    @Test
    public void errorOnBuildingProducerTest() {
        KafkaITContext context = KafkaITContext.produceVM();
        context.setObjectTypeId(_topic);

        // invalid property to trigger error while building the Producer
        context.addOperationProperty(Constants.KEY_COMPRESSION_TYPE, "invalid");

        KafkaOperationConnection connection = TestUtils.createOperationConnection(context);
        ProduceOperation operation = new ProduceOperation(connection);

        SimpleTrackedData data = buildData(0, "payload", "key1");
        SimpleOperationResponse response = ResponseFactory.get(data);
        UpdateRequest request = TestUtils.stubRequest(data);

        // execute SUT
        operation.executeUpdate(request, response);

        List<SimpleOperationResult> results = response.getResults();

        Assert.assertEquals(1, results.size());

        SimpleOperationResult result = CollectionUtil.getFirst(results);
        Assert.assertEquals(OperationStatus.FAILURE, result.getStatus());
    }

    @Test
    public void shouldFailForDynamicTopicWithoutTopicDocumentPropertyTest() {
        KafkaITContext context = KafkaITContext.produceVM();
        context.setObjectTypeId(Constants.DYNAMIC_TOPIC_ID);

        KafkaOperationConnection connection = TestUtils.createOperationConnection(context);
        ProduceOperation operation = new ProduceOperation(connection);

        SimpleTrackedData dataWithoutTopic = buildData(0, "invalid topic", "key1");

        SimpleOperationResponse response = ResponseFactory.get(dataWithoutTopic);
        UpdateRequest request = TestUtils.stubRequest(dataWithoutTopic);

        // execute SUT
        operation.executeUpdate(request, response);

        List<SimpleOperationResult> results = response.getResults();

        Assert.assertEquals(1, results.size());

        SimpleOperationResult result = CollectionUtil.getFirst(results);
        Assert.assertEquals(OperationStatus.APPLICATION_ERROR, result.getStatus());
        Assert.assertEquals(Constants.CODE_ERROR, result.getStatusCode());
    }

    @Test
    public void shouldIgnoreTopicFromDocumentPropertyTest() {
        KafkaITContext context = KafkaITContext.produceVM();
        context.setObjectTypeId(_topic);

        KafkaOperationConnection connection = TestUtils.createOperationConnection(context);
        ProduceOperation operation = new ProduceOperation(connection);

        SimpleTrackedData data = buildData(0, "valid topic", "key1", "INVALID_TOPIC");

        SimpleOperationResponse response = ResponseFactory.get(data);
        UpdateRequest request = TestUtils.stubRequest(data);

        // execute SUT
        operation.executeUpdate(request, response);

        List<SimpleOperationResult> results = response.getResults();

        Assert.assertEquals(1, results.size());

        SimpleOperationResult success = CollectionUtil.getFirst(results);
        Assert.assertEquals(OperationStatus.SUCCESS, success.getStatus());
    }

    @Test
    public void overrideTopicFromDocumentPropertyTest() {
        KafkaITContext context = KafkaITContext.produceVM();
        context.setObjectTypeId(Constants.DYNAMIC_TOPIC_ID);

        KafkaOperationConnection connection = TestUtils.createOperationConnection(context);
        ProduceOperation operation = new ProduceOperation(connection);

        SimpleTrackedData data = buildData(0, "valid topic", "key1", _topic);

        SimpleOperationResponse response = ResponseFactory.get(data);
        UpdateRequest request = TestUtils.stubRequest(data);

        // execute SUT
        operation.executeUpdate(request, response);

        List<SimpleOperationResult> results = response.getResults();

        Assert.assertEquals(1, results.size());

        SimpleOperationResult success = CollectionUtil.getFirst(results);
        Assert.assertEquals(OperationStatus.SUCCESS, success.getStatus());
    }

    @Test
    public void overrideTopicFromDynamicTopicFieldTest() {
        KafkaITContext context = KafkaITContext.produceVM();
        context.setObjectTypeId(Constants.DYNAMIC_TOPIC_ID);
        context.addOperationProperty(Constants.TOPIC_NAME_FIELD_ID, _topic);

        KafkaOperationConnection connection = TestUtils.createOperationConnection(context);
        ProduceOperation operation = new ProduceOperation(connection);

        SimpleTrackedData data = buildData(0, "valid topic", "key1", "INVALID_TOPIC");

        SimpleOperationResponse response = ResponseFactory.get(data);
        UpdateRequest request = TestUtils.stubRequest(data);

        // execute SUT
        operation.executeUpdate(request, response);

        List<SimpleOperationResult> results = response.getResults();

        Assert.assertEquals(1, results.size());

        SimpleOperationResult success = CollectionUtil.getFirst(results);
        Assert.assertEquals(OperationStatus.SUCCESS, success.getStatus());
    }

    @Test
    public void overrideTopicFromDocumentPropertyIfEmptyDynamicTopicFieldTest() {
        KafkaITContext context = KafkaITContext.produceVM();
        context.setObjectTypeId(Constants.DYNAMIC_TOPIC_ID);
        context.addOperationProperty(Constants.TOPIC_NAME_FIELD_ID, "");

        KafkaOperationConnection connection = TestUtils.createOperationConnection(context);
        ProduceOperation operation = new ProduceOperation(connection);

        SimpleTrackedData data = buildData(0, "valid topic", "key1", _topic);

        SimpleOperationResponse response = ResponseFactory.get(data);
        UpdateRequest request = TestUtils.stubRequest(data);

        // execute SUT
        operation.executeUpdate(request, response);

        List<SimpleOperationResult> results = response.getResults();

        Assert.assertEquals(1, results.size());

        SimpleOperationResult success = CollectionUtil.getFirst(results);
        Assert.assertEquals(OperationStatus.SUCCESS, success.getStatus());
    }

    @Test
    public void invalidDynamicTopic() {
        KafkaITContext context = KafkaITContext.produceVM();
        context.setObjectTypeId(Constants.DYNAMIC_TOPIC_ID);
        context.addOperationProperty(Constants.KEY_MAXIMUM_TIME_TO_WAIT, 1000L);

        KafkaOperationConnection connection = TestUtils.createOperationConnection(context);
        ProduceOperation operation = new ProduceOperation(connection);

        SimpleTrackedData data = buildData(0, "valid topic", "key1", "invalidMe");

        SimpleOperationResponse response = ResponseFactory.get(data);
        UpdateRequest request = TestUtils.stubRequest(data);

        // execute SUT
        operation.executeUpdate(request, response);

        List<SimpleOperationResult> results = response.getResults();

        Assert.assertEquals(1, results.size());

        SimpleOperationResult result = CollectionUtil.getFirst(results);
        Assert.assertEquals(OperationStatus.APPLICATION_ERROR, result.getStatus());
        Assert.assertEquals(Constants.CODE_UNKNOWN_TOPIC, result.getStatusCode());
    }

    @Test
    public void invalidStaticTopic() {
        KafkaITContext context = KafkaITContext.produceVM();
        context.setObjectTypeId("invalidMe");
        context.addOperationProperty(Constants.KEY_MAXIMUM_TIME_TO_WAIT, 1000L);

        KafkaOperationConnection connection = TestUtils.createOperationConnection(context);
        ProduceOperation operation = new ProduceOperation(connection);

        SimpleTrackedData data = buildData(0, "valid topic", "key1");

        SimpleOperationResponse response = ResponseFactory.get(data);
        UpdateRequest request = TestUtils.stubRequest(data);

        // execute SUT
        operation.executeUpdate(request, response);

        List<SimpleOperationResult> results = response.getResults();

        Assert.assertEquals(1, results.size());

        SimpleOperationResult result = CollectionUtil.getFirst(results);
        Assert.assertEquals(OperationStatus.FAILURE, result.getStatus());
        Assert.assertEquals(Constants.CODE_UNKNOWN_TOPIC, result.getStatusCode());
    }

}
