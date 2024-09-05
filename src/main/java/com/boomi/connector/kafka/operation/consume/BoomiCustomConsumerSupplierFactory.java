package com.boomi.connector.kafka.operation.consume;

import com.boomi.connector.kafka.client.consumer.BoomiCustomConsumer;
import com.boomi.connector.kafka.client.consumer.ConsumerConfiguration;

import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * Factory class responsible for creating a supplier for {@link BoomiCustomConsumer}, and subscribe to a specific topic,
 * or assign one or multiples partitions to a specific topic.
 */
public final class BoomiCustomConsumerSupplierFactory {

    private BoomiCustomConsumerSupplierFactory() {
    }

    /**
     * Create a {@link BoomiCustomConsumer} and assign the given topic partitions.
     *
     * @param config
     *         The configuration
     * @param topicPartitions
     *         List of topic name and partition number
     * @return a Supplier of {@link BoomiCustomConsumer} with the assigned partitions.
     */
    public static Supplier<BoomiCustomConsumer> createSupplier(ConsumerConfiguration config,
            Collection<TopicPartition> topicPartitions) {
        return () -> {
            BoomiCustomConsumer consumer = new BoomiCustomConsumer(config);
            consumer.assign(topicPartitions);
            return consumer;
        };
    }

    /**
     * Create a {@link BoomiCustomConsumer} and subscribe the given topic.
     *
     * @param config
     *         The configuration
     * @param topic
     *         name
     * @return a Supplier of {@link BoomiCustomConsumer} with the subscribed topic.
     */
    public static Supplier<BoomiCustomConsumer> createSupplier(ConsumerConfiguration config, String topic) {
        return () -> {
            BoomiCustomConsumer consumer = new BoomiCustomConsumer(config);
            consumer.subscribe(topic);
            return consumer;
        };
    }

    public static Supplier<BoomiCustomConsumer> createSupplierRegex(ConsumerConfiguration config, String regex) {
        return () -> {
            BoomiCustomConsumer consumer = new BoomiCustomConsumer(config);
            consumer.subscribeWithPattern(Pattern.compile(regex));
            return consumer;
        };
    }
}