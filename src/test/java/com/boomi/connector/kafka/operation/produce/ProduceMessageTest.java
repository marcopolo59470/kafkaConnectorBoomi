//Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.kafka.operation.produce;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.kafka.exception.InvalidMessageSizeException;
import com.boomi.connector.testutil.MutableDynamicPropertyMap;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.boomi.util.StreamUtil;
import com.boomi.util.StringUtil;
import com.boomi.util.io.FastByteArrayInputStream;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(Theories.class)
public class ProduceMessageTest {

    private static final int UNLIMITED_SIZE = Integer.MAX_VALUE;
    private static final String DYNAMIC_TOPIC = "DYNAMIC_TOPIC";
    private static final String HEADER_PROPERTIES_KEY = "header_properties";
    private static final String PARTITION_ID_KEY = "partition_id";

    private static String toString(ProducerRecord<?, MessagePayload> record) throws IOException {
        return StreamUtil.toString(record.value().getInputStream(), StringUtil.UTF8_CHARSET);
    }

    /**
     * Build a list of Test Cases to be used by {@link ProduceMessageTest#produceMessageTests}:
     * <ul>
     *     <li>Object data without custom dynamic document properties nor documents properties</li>
     *     <li>Object data with DYNAMIC_TOPIC and the new topic in the document properties</li>
     *     <li>Object data with custom dynamic document properties to be send as headers</li>
     *     <li>Object data with DYNAMIC_TOPIC and the new topic in the document properties and custom dynamic
     *     document properties to be send as headers</li>
     * </ul>
     *
     * @return the test cases
     */
    @DataPoints("testCases")
    public static List<TestCase> testCases() {
        final String payload = "some random payload";
        final String topic = "some topic name";

        // without message headers, without dynamic topic
        final TestCase withoutDynamicTopic = new TestCase(payload).withTopic(topic).withPartition(1);

        // without message headers, with dynamic topic
        final TestCase withDynamicTopic = new TestCase(payload).withDynamicTopic(topic);

        return Arrays.asList(withoutDynamicTopic, withDynamicTopic);
    }

    /**
     * Run the test cases specified on {@link ProduceMessageTest#testCases}
     *
     * @param testCase
     *         to run
     * @throws InvalidMessageSizeException
     *         should not throw this
     * @throws IOException
     *         should not throw this
     */
    @Theory
    public void produceMessageTests(@FromDataPoints("testCases") TestCase testCase)
            throws InvalidMessageSizeException, IOException {
        final ObjectData objectData = testCase.getObjectData();

        ProduceMessage message = new ProduceMessage(objectData, testCase._topic, UNLIMITED_SIZE);
        ProducerRecord<String, MessagePayload> record = message.toRecord();
        Header[] header = record.headers().toArray();

        Assert.assertEquals(testCase._actualTopic, record.topic());
        Assert.assertEquals(testCase._payload, toString(record));
        Assert.assertEquals(1 ,header.length);
        Assert.assertEquals("header_key", header[0].key());
        Assert.assertEquals("header_value", new String(header[0].value(), StringUtil.UTF8_CHARSET));
        Assert.assertEquals(testCase._partitionId, record.partition());
    }

    /**
     * Verify the construction of {@link ProduceMessage} fails when providing a payload larger than the maximum allowed
     * size
     *
     * @throws InvalidMessageSizeException
     *         for payloads larger than maximum allowed size
     */
    @Test(expected = InvalidMessageSizeException.class)
    public void invalidPayloadSizeTest() throws InvalidMessageSizeException {
        final byte[] bytes = "some data exceeding the size limit".getBytes(StringUtil.UTF8_CHARSET);
        InputStream stream = new FastByteArrayInputStream(bytes);

        ObjectData objectData = new SimpleTrackedData(0, stream);

        final int sizeSmallerThanPayload = bytes.length - 1;
        new ProduceMessage(objectData, "random topic", sizeSmallerThanPayload);
    }

    private static class TestCase {

        final String _payload;
        final Map<String, String> _documentProperties = new HashMap<>(1);
        final MutableDynamicPropertyMap _dynamicOpProps = new MutableDynamicPropertyMap();

        String _topic;
        String _actualTopic;
        Integer _partitionId;

        TestCase(String payload) {
            _payload = payload;
            final Map<String, String> _header = new HashMap<>(1);
            _header.put("header_key", "header_value");
            _dynamicOpProps.addProperty(HEADER_PROPERTIES_KEY, _header);
        }

        TestCase withTopic(String topic) {
            _topic = topic;
            _actualTopic = topic;
            return this;
        }

        TestCase withDynamicTopic(String topic) {
            _topic = DYNAMIC_TOPIC;
            _actualTopic = topic;
            _documentProperties.put("topic_name", topic);
            return this;
        }

        TestCase withPartition(Integer partitionId) {
            _partitionId = partitionId;
            _dynamicOpProps.addProperty(PARTITION_ID_KEY, partitionId);
            return this;
        }

        ObjectData getObjectData() {
            final InputStream data = new FastByteArrayInputStream(_payload.getBytes(StringUtil.UTF8_CHARSET));
            return new SimpleTrackedData(0, data, Collections.<String, String>emptyMap(), _documentProperties,
                    _dynamicOpProps);
        }
    }
}
