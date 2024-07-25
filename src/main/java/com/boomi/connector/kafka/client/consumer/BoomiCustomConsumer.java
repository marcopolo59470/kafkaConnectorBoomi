package com.boomi.connector.kafka.client.consumer;

import com.boomi.connector.kafka.operation.commit.Committable;
import com.boomi.util.CollectionUtil;


import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;

import java.io.InputStream;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

/**
 * An extension of {@link KafkaConsumer} with custom method overloads.
 */
public class BoomiCustomConsumer extends KafkaConsumer<Object, InputStream> {

    private boolean _assignPartitions;

    public BoomiCustomConsumer(ConsumerConfiguration configuration) {
        super(configuration.getConfig(), configuration.getClientId(), configuration.getChannelBuilder(),
                configuration.getMaxRequestSize());
    }

    /**public BoomiCustomConsumer(Properties configuration, Deserializer<String> keyDeserializer, Deserializer<GenericRecord> valueDeserializer) {
        super(configuration, keyDeserializer, valueDeserializer);
    }*/


    /**
     * Fetches a batch of messages from the subscribed topic.
     *
     * @param timeout
     *         The time, in milliseconds, spent waiting in poll if data is not available in the buffer. If 0, returns
     *         immediately with any records that are available currently in the buffer, else returns empty. Must not be
     *         negative.
     * @return a batch of messages.
     */
    public Iterable<ConsumeMessage> pollMessages(long timeout) {
        return CollectionUtil.apply(poll(Duration.ofMillis(timeout)),
                new CollectionUtil.Function<ConsumerRecord<Object, InputStream>, ConsumeMessage>() {
                    @Override
                    public ConsumeMessage apply(ConsumerRecord<Object, InputStream> record) {
                        return new ConsumeMessage(record);
                    }
                });
    }

    /**
     * Commit the given {@link Committable}.
     *
     * @param message
     *         the message to commit.
     */
    public void commit(Committable message) {
        boolean success = synchronizedCommit(
                Collections.singletonMap(message.getTopicPartition(), message.getNextOffset()));
        if (!success) {
            throw new KafkaException("Could not reach the server to commit offsets");
        }
    }

    /**
     * Subscribe to the given topic.
     *
     * @param topic
     *         to subscribe to.
     */
    public void subscribe(String topic) {
        subscribe(Collections.singleton(topic));
    }

    /**
     * Manually assign a list of partitions to this consumer.
     *
     * @param partitions the list of partitions to assign this consumer
     * @throws IllegalArgumentException - If partitions is null or contains null or empty topics
     * @throws IllegalStateException    - If subscribe() was called previously
     */
    @Override
    public void assign(Collection<TopicPartition> partitions) {
        super.assign(partitions);
        _assignPartitions = true;
    }

    /**
     * Check if the consumer is manually assigned partitions  to a Topic.
     *
     * @return true if a partition(s) is manually assigned, false otherwise.
     */
    public boolean isAssignPartitions() {
        return _assignPartitions;
    }
}
