// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.kafka.client.consumer;

import com.boomi.connector.api.Payload;
import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.api.PayloadUtil;
import com.boomi.connector.kafka.operation.commit.Committable;
import com.boomi.connector.kafka.util.Constants;
import com.boomi.util.IOUtil;
import com.boomi.util.StreamUtil;
import com.boomi.util.StringUtil;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.io.Closeable;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Representation of the messages consumed from Kafka
 */
public class ConsumeMessage implements Committable, Closeable {

    private static final String CUSTOM_HEADER_PROPERTIES_GROUP_ID = "custom_header_properties";
    private final long _offset;
    private final String _key;
    private final TopicPartition _topicPartition;
    private final InputStream _message;
    private final Headers _headers;
    private final Long _timestamp;

    /**
     * Construct a new instance of {@link ConsumeMessage} from the attributes of the given {@link ConsumerRecord}.
     *
     * @param record
     *         containing the message attributes
     */
    public ConsumeMessage(ConsumerRecord<String, InputStream> record) {
        this(new TopicPartition(record.topic(), record.partition()), record.offset(), record.key(), record.value(),
                record.headers(), record.timestamp());
    }

    /**
     * This constructor is used to create a Message without payload that only holds the metadata associated with the
     * topic, partition and offset
     *
     * @param topicPartition
     *         containing the topic name and partition number
     * @param offset
     *         the position of the message in the partition
     */
    public ConsumeMessage(TopicPartition topicPartition, long offset) {
        this(topicPartition, offset, null, StreamUtil.EMPTY_STREAM, new RecordHeaders(), null);
    }

    private ConsumeMessage(TopicPartition topicPartition, long offset, String key, InputStream message,
            Headers headers, Long timestamp) {
        _topicPartition = topicPartition;
        _offset = offset;
        _key = key;
        _message = message;
        _headers = headers;
        _timestamp = timestamp;
    }

    /**
     * Builds a {@link Payload} containing the message as input and its metadata as Tracked Properties
     *
     * @return the payload
     */
    public Payload toPayload(PayloadMetadata metadata) {
        metadata.setTrackedProperty(Constants.KEY_TOPIC_NAME, _topicPartition.topic());
        metadata.setTrackedProperty(Constants.KEY_MESSAGE_OFFSET, String.valueOf(_offset));
        metadata.setTrackedProperty(Constants.KEY_TOPIC_PARTITION, String.valueOf(_topicPartition.partition()));
        metadata.setTrackedProperty(Constants.KEY_MESSAGE_KEY, _key);

        String timestamp = _timestamp == null ? StringUtil.EMPTY_STRING : String.valueOf(_timestamp);
        metadata.setTrackedProperty(Constants.KEY_MESSAGE_TIMESTAMP, timestamp);

        Map<String, String> propertyGroup = new HashMap<>();
        for (Header header : _headers) {
            propertyGroup.put(header.key(), new String(header.value(), StringUtil.UTF8_CHARSET));
        }
        metadata.setTrackedGroupProperties(CUSTOM_HEADER_PROPERTIES_GROUP_ID, propertyGroup);

        return PayloadUtil.toPayload(_message, metadata);
    }

    /**
     * @return the {@link TopicPartition} where this message come from
     */
    @Override
    public TopicPartition getTopicPartition() {
        return _topicPartition;
    }

    /**
     * @return the offset position to the next message
     */
    @Override
    public OffsetAndMetadata getNextOffset() {
        return new OffsetAndMetadata(_offset + 1);
    }

    /**
     * @return the offset position of this message
     */
    public OffsetAndMetadata getOffset() {
        return new OffsetAndMetadata(_offset);
    }

    @Override
    public void close() {
        IOUtil.closeQuietly(_message);
    }
}
