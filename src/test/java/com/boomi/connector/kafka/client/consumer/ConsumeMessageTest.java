// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.kafka.client.consumer;

import com.boomi.connector.api.ExtendedPayload;
import com.boomi.connector.api.Payload;
import com.boomi.connector.testutil.SimplePayloadMetadata;
import com.boomi.util.StreamUtil;
import com.boomi.util.StringUtil;
import com.boomi.util.io.FastByteArrayInputStream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

public class ConsumeMessageTest {

    private static ConsumerRecord<String, InputStream> buildRecord(long timestamp, String topic, int partition,
            long offset, String key, InputStream value) {
        final int keySize = 0; // doesn't matter for our tests
        final int valueSize = 0; // doesn't matter for our tests
        final Headers headers = new RecordHeaders();
        headers.add(new RecordHeader("header_key", "header_value".getBytes(StandardCharsets.UTF_8)));

        return new ConsumerRecord<>(topic, partition, offset, timestamp, TimestampType.CREATE_TIME, keySize, valueSize,
                key, value, headers, Optional.empty());
    }

    @Test
    public void nextOffsetTest() {
        final String topic = "some topic";
        final int partition = 0;
        final long offset = 0L;

        try (ConsumeMessage message = new ConsumeMessage(new TopicPartition(topic, partition), offset)) {
            final OffsetAndMetadata offsetMetadata = message.getNextOffset();
            Assert.assertNotNull(offsetMetadata);

            final long nextOffset = offset + 1;
            Assert.assertEquals(nextOffset, offsetMetadata.offset());
        }
    }

    @Test
    public void constructFromConsumerRecordTest() throws IOException {
        // topic data
        final String topic = "Topic for Tests";
        final int partition = 32;

        // message content
        final String content = "the message payload";
        InputStream body = new FastByteArrayInputStream(content.getBytes(StringUtil.UTF8_CHARSET));

        // message properties
        final long offset = 42L;
        final String key = "the message key";
        final long timestamp = System.currentTimeMillis();

        // construct consumer record from the previously defined values
        final ConsumerRecord<String, InputStream> record = buildRecord(timestamp, topic, partition, offset, key, body);

        // construct the message & get a payload from it
        Payload payload;
        try (ConsumeMessage message = new ConsumeMessage(record)) {
            payload = message.toPayload(new SimplePayloadMetadata());
        }

        // assert the message content
        final String messageContent = StreamUtil.toString(payload.readFrom(), StringUtil.UTF8_CHARSET);
        Assert.assertEquals(content, messageContent);

        // assert the message properties
        final SimplePayloadMetadata metadata = (SimplePayloadMetadata) ((ExtendedPayload) payload).getMetadata();
        final Map<String, String> trackedProps = metadata.getTrackedProps();
        final Map<String, Map<String, String>> trackedGroups = metadata.getTrackedGroups();
        final Map<String, String> headerTracked = trackedGroups.get("custom_header_properties");

        Assert.assertEquals(topic, trackedProps.get("topic_name"));
        Assert.assertEquals(String.valueOf(partition), trackedProps.get("topic_partition"));
        Assert.assertEquals(String.valueOf(offset), trackedProps.get("message_offset"));
        Assert.assertEquals(key, trackedProps.get("message_key"));
        Assert.assertEquals(String.valueOf(timestamp), trackedProps.get("message_timestamp"));
        Assert.assertEquals("header_value", headerTracked.get("header_key"));
    }
}
