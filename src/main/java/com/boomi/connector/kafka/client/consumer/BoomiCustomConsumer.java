package com.boomi.connector.kafka.client.consumer;

import com.boomi.connector.kafka.operation.commit.Committable;
import com.boomi.util.CollectionUtil;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Set;

import java.io.InputStream;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * An extension of {@link KafkaConsumer} with custom method overloads.
 */
public class BoomiCustomConsumer implements AutoCloseable { // Implémentation d'AutoCloseable

    private final KafkaConsumer<Object, InputStream> kafkaConsumer;
    private boolean assignPartitions;

    /**
     * Constructor to initialize KafkaConsumer with the provided configuration.
     *
     * @param configuration the consumer configuration.
     */
    public BoomiCustomConsumer(ConsumerConfiguration configuration) {
        // Build properties from ConsumerConfiguration
        Properties props = new Properties();
        props.putAll(configuration.getConfig().originals());
        props.put("client.id", configuration.getClientId());
        props.put("max.partition.fetch.bytes", configuration.getMaxRequestSize());

        // Initialize KafkaConsumer
        this.kafkaConsumer = new KafkaConsumer<>(props);
    }

    /**
     * Fetches a batch of messages from the subscribed topic.
     *
     * @param timeout The time, in milliseconds, spent waiting in poll if data is not available in the buffer.
     * @return a batch of messages.
     */
    public Iterable<ConsumeMessage> pollMessages(long timeout) {
        return CollectionUtil.apply(
                kafkaConsumer.poll(Duration.ofMillis(timeout)),
                new CollectionUtil.Function<ConsumerRecord<Object, InputStream>, ConsumeMessage>() {
                    @Override
                    public ConsumeMessage apply(ConsumerRecord<Object, InputStream> record) {
                        return new ConsumeMessage(record);
                    }
                }
        );
    }

    /**
     * Commit the given {@link Committable}.
     *
     * @param message the message to commit.
     */
    public void commit(Committable message) {
        try {
            kafkaConsumer.commitSync(
                    Collections.singletonMap(message.getTopicPartition(), message.getNextOffset())
            );
        } catch (Exception e) {
            throw new KafkaException("Could not reach the server to commit offsets", e);
        }
    }

    /**
     * Subscribe to the given topic.
     *
     * @param topic to subscribe to.
     */
    public void subscribe(String topic) {
        kafkaConsumer.subscribe(Collections.singleton(topic));
    }

    /**
     * Subscribe to the topics matching the provided pattern.
     *
     * @param pattern used to search matching topics.
     */
    public void subscribeWithPattern(Pattern pattern) {
        kafkaConsumer.subscribe(pattern);
    }

    /**
     * Manually assign a list of partitions to this consumer.
     *
     * @param partitions the list of partitions to assign to this consumer.
     * @throws IllegalArgumentException If partitions is null or contains null or empty topics.
     * @throws IllegalStateException    If subscribe() was called previously.
     */
    public void assign(Collection<TopicPartition> partitions) {
        kafkaConsumer.assign(partitions);
        assignPartitions = true;
    }

    /**
     * Check if the consumer has manually assigned partitions to a Topic.
     *
     * @return true if partitions are manually assigned, false otherwise.
     */
    public boolean isAssignPartitions() {
        return assignPartitions;
    }

    /**
     * Close the consumer gracefully.
     */
    @Override
    public void close() {
        if (kafkaConsumer != null) {
            kafkaConsumer.close();
        }
    }

    /**
     * Get the underlying KafkaConsumer instance.
     *
     * @return KafkaConsumer instance.
     */
    public KafkaConsumer<Object, InputStream> getKafkaConsumer() {
        return this.kafkaConsumer;
    }

    public Set<TopicPartition> assignment() {
        return kafkaConsumer.assignment();
    }

    public long position(TopicPartition partition) {
        return kafkaConsumer.position(partition);
    }

    public void pause(Collection<TopicPartition> partitions) {
        kafkaConsumer.pause(partitions);
    }

    public void resume(Collection<TopicPartition> partitions) {
        kafkaConsumer.resume(partitions);
    }

    public ConsumerRecords<Object, InputStream> poll(Duration timeout, boolean includeMetadata) {
        return kafkaConsumer.poll(timeout);
    }

    public void seek(TopicPartition partition, long offset) {
        kafkaConsumer.seek(partition, offset);
    }

    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        kafkaConsumer.commitAsync(offsets, callback);
    }

    public boolean isSubscribed() {
        return !kafkaConsumer.subscription().isEmpty();
    }


}
