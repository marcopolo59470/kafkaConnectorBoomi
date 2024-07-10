package com.boomi.connector.kafka.client.common.serialization;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.kafka.operation.produce.MessagePayload;
import com.boomi.util.StreamUtil;

import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Implementation of {@link Serializer} for serializing an {@link InputStream} to a byte[].
 * This implementation does not handle the closing of the given InputStream after serializing it.
 *
 * Note that due to Apache Kafka Library implementation, the whole content of the {@link InputStream} will be loaded
 * in memory. Because of this, it is expected to receive a content size that can be handled without causing an
 * {@link OutOfMemoryError}.
 */
public class InputStreamSerializer implements Serializer<MessagePayload> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // no-op
    }

    @Override
    public byte[] serialize(String topic, MessagePayload messagePayload) {
        try {
            int size = messagePayload.getSize();
            byte[] payload = new byte[size];
            StreamUtil.readFully(messagePayload.getInputStream(), payload);
            return payload;
        } catch (IOException e) {
            throw new ConnectorException("error serializing input data", e);
        }
    }

    @Override
    public void close() {
        // no-op
    }
}
