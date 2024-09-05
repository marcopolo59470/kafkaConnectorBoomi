package com.boomi.connector.kafka.operation.polling;

import com.boomi.connector.kafka.client.consumer.BoomiCustomConsumer;
import com.boomi.connector.kafka.client.consumer.ConsumeMessage;
import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import java.io.Closeable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A wrapper for {@link BoomiCustomConsumer} with helper methods to commit and rewind the message offsets
 */
public class BoomiListenerConsumer implements Closeable {

    private static final Logger LOG = LogUtil.getLogger(BoomiListenerConsumer.class);
    private static final KafkaCommitCallback COMMIT_CALLBACK = new KafkaCommitCallback();
    // set the Poll timeout to 0 seconds to avoid waiting for new messages
    private static final long NO_WAIT = 0L;
    private static final boolean INCLUDE_METADATA_IN_TIMEOUT = false;

    private final Supplier<BoomiCustomConsumer> _consumerSupplier;
    private BoomiCustomConsumer _consumer;

    BoomiListenerConsumer(Supplier<BoomiCustomConsumer> consumerSupplier) {
        _consumerSupplier = consumerSupplier;
        _consumer = consumerSupplier.get();
    }

    /**
     * Subscribes to the topic provided when this object was constructed
     */
    void subscribe() {
        IOUtil.closeQuietly(_consumer);

        boolean isSuccess = false;
        try {
            _consumer = _consumerSupplier.get();

            // there is no need to invoke joinGroup() when isAssignPartitions() is true, as the partitions are
            // already assigned
            if (!_consumer.isAssignPartitions()) {
                // communicate with the service to force a partition rebalance, this ensure that the consumer will
                // obtain any available partitions
                joinGroup();
            }
            isSuccess = true;
        } finally {
            if (!isSuccess) {
                IOUtil.closeQuietly(_consumer);
            }
        }
    }

    /**
     * Execute a poll request while being in pause to make this consumer join the group without retrieving any message
     */
    private void joinGroup() {
        _consumer.pause(_consumer.assignment());
        _consumer.poll(Duration.ofMillis(NO_WAIT), INCLUDE_METADATA_IN_TIMEOUT);
        _consumer.resume(_consumer.assignment());
    }

    /**
     * Fetches a batch of messages from the subscribed topic.
     *
     * @return a batch of messages
     */
    Iterable<ConsumeMessage> poll() {
        return _consumer.pollMessages(NO_WAIT);
    }

    /**
     * Rewind to the given assignment.
     *
     * @param startOffsets
     *         a map containing the offsets to rewind each partition
     */
    void rewind(Map<TopicPartition, OffsetAndMetadata> startOffsets) {

        for (TopicPartition partition : _consumer.assignment()) {
            OffsetAndMetadata metadata = startOffsets.get(partition);
            if (metadata != null) {
                seekOffset(partition, metadata.offset());
            }
        }
    }

    private void seekOffset(TopicPartition partition, long offset) {
        try {
            _consumer.seek(partition, offset);
        } catch (IllegalStateException e) {
            // A re-balance took place and this consumer is not longer assigned to the provided partition. A retry will
            // not be possible but this doesn't necessarily implies losing messages as a new consumer will begin from
            // the last committed offset.
            // This exception is logged and the execution allowed to continue
            LOG.log(Level.WARNING, e.getMessage(), e);
        }
    }

    /**
     * Executes an asynchronous commit of the given offsets.
     *
     * @param lastOffsets
     *         a Map with the offsets to commit.
     */
    void commit(Map<TopicPartition, OffsetAndMetadata> lastOffsets) {
        Map<TopicPartition, OffsetAndMetadata> positions = new HashMap<>();
        String topic = null;
        for (TopicPartition partition : _consumer.assignment()) {
            topic = partition.topic();
            OffsetAndMetadata metadata = lastOffsets.get(partition);
            if (metadata != null) {
                positions.put(partition, metadata);
            }
        }

        LOG.log(Level.INFO, "Committing offsets for topic: {0}, offsets: {1}", new Object[] { topic, positions });
        _consumer.commitAsync(positions, COMMIT_CALLBACK);
    }

    @Override
    public void close() {
        IOUtil.closeQuietly(_consumer);
    }

    /**
     * Check if the consumer is subscribed or manually assigned to a Topic
     *
     * @return true if a topic subscription exist, false otherwise
     */
    boolean isSubscribed() {
        return _consumer.isAssignPartitions() || _consumer.isSubscribed();
    }

    /**
     * Custom {@link OffsetCommitCallback} to get the final state of a Commit Offset request. If the commit has failed,
     * this class will simply log the offsets and allow the consumer to continue as this situation will be remedied on
     * next executions as follows: - If the request has failed for a temporary reason but the consumer retains its
     * current assignments, then the offsets belonging to a following batch will be eventually committed and make this
     * request obsolete. - If the failure was caused because this consumer was taken out of rotation and has lost these
     * assignments, then retrying it is futile as another consumer would have taken the partitions and re-processed this
     * batch.
     */
    private static class KafkaCommitCallback implements OffsetCommitCallback {

        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
            if (e != null) {
                LOG.log(Level.WARNING, e, () -> "Couldn't commit offsets: " + map);
            }
        }
    }
}
