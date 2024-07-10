
package com.boomi.connector.kafka.operation.commit;

import com.boomi.connector.kafka.client.consumer.BoomiCustomConsumer;
import com.boomi.connector.kafka.client.consumer.ConsumerConfiguration;
import com.boomi.util.IOUtil;

import java.io.Closeable;

public class BoomiCommitter implements Closeable {

    private final BoomiCustomConsumer _committer;

    public BoomiCommitter(ConsumerConfiguration boomiConfiguration) {
        _committer = new BoomiCustomConsumer(boomiConfiguration);
    }

    /**
     * Commit the given {@link Committable}.
     *
     * @param message
     *         the message to commit.
     */
    void commit(Committable message) {
        _committer.commit(message);
    }

    @Override
    public void close() {
        IOUtil.closeQuietly(_committer);
    }
}
