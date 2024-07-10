package com.boomi.connector.kafka.client.common.serialization;

import com.boomi.util.io.FastByteArrayInputStream;

import org.apache.kafka.common.serialization.Deserializer;

import java.io.InputStream;
import java.util.Map;

/**
 * Implementation of {@link Deserializer} for deserializing a byte[] to an {@link InputStream}.
 * This implementation returns a {@link FastByteArrayInputStream} wrapping the given byte array.
 */
public class InputStreamDeserializer implements Deserializer<InputStream> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // no-op
    }

    @Override
    public InputStream deserialize(String topic, byte[] data) {
        return new FastByteArrayInputStream(data);
    }

    @Override
    public void close() {
        // no-op
    }
}
