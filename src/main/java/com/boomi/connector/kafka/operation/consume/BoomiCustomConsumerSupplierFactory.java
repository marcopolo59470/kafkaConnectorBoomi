package com.boomi.connector.kafka.operation.consume;

import com.boomi.connector.kafka.client.consumer.BoomiCustomConsumer;
import com.boomi.connector.kafka.client.consumer.ConsumerConfiguration;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

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

    static LogContext logContext;

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
            logContext = new LogContext(String.format("[Producer Boomi Listener] "));
            ChannelBuilder channelBuilder = createChannelBuilder(config.getConfig(), Time.SYSTEM, logContext);
            BoomiCustomConsumer consumer = new BoomiCustomConsumer(config, channelBuilder);
            consumer.assign(topicPartitions);
            return consumer;
        };
    }

    public static ChannelBuilder createChannelBuilder(AbstractConfig config, Time time, LogContext logContext) {
        SecurityProtocol securityProtocol = SecurityProtocol.forName(config.getString("security.protocol"));
        String clientSaslMechanism = config.getString("sasl.mechanism");
        return ChannelBuilders.clientChannelBuilder(securityProtocol, JaasContext.Type.CLIENT, config, (ListenerName)null, clientSaslMechanism, time, true, logContext);
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
            BoomiCustomConsumer consumer = new BoomiCustomConsumer(config, null);
            consumer.subscribe(topic);
            return consumer;
        };
    }

    public static Supplier<BoomiCustomConsumer> createSupplierRegex(ConsumerConfiguration config, String regex) {
        return () -> {
            BoomiCustomConsumer consumer = new BoomiCustomConsumer(config, null);
            consumer.subscribeWithPattern(Pattern.compile(regex));
            return consumer;
        };
    }
}