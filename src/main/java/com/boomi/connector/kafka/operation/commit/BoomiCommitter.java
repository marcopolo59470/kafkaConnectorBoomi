package com.boomi.connector.kafka.operation.commit;

import com.boomi.connector.kafka.client.consumer.BoomiCustomConsumer;
import com.boomi.connector.kafka.client.consumer.ConsumerConfiguration;

import java.io.Closeable;

public class BoomiCommitter implements Closeable {

    private final BoomiCustomConsumer _committer;

    public BoomiCommitter(ConsumerConfiguration boomiConfiguration) {
        _committer = new BoomiCustomConsumer(boomiConfiguration);
    }

    /**
     * Commit the given {@link Committable}.
     *
     * @param message the message to commit.
     */
    void commit(Committable message) {
        _committer.commit(message);
    }

    @Override
    public void close() {
        if (_committer != null) {
            try {
                _committer.close();
            } catch (Exception e) {
                // Log the exception or handle it quietly
                e.printStackTrace(); // Replace with LOG for proper logging
            }
        }
    }
}
