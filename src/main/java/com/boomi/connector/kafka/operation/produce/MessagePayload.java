
package com.boomi.connector.kafka.operation.produce;

import com.boomi.util.IOUtil;

import java.io.Closeable;
import java.io.InputStream;

/**
 * Representation of the message content to publish to Apache Kafka service.
 */
public class MessagePayload implements Closeable {

    private InputStream _inputStream;
    private int _size;

    MessagePayload(InputStream inputStream, int size) {
        _inputStream = inputStream;
        _size = size;
    }

    public InputStream getInputStream() {
        return _inputStream;
    }

    public int getSize() {
        return _size;
    }

    @Override
    public void close() {
        IOUtil.closeQuietly(_inputStream);
    }
}
