// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.kafka.operation;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.kafka.KafkaConnection;
import com.boomi.connector.kafka.client.consumer.BoomiCustomConsumer;
import com.boomi.connector.kafka.client.consumer.ConsumerConfiguration;
import com.boomi.connector.kafka.client.producer.ProducerConfiguration;
import com.boomi.connector.kafka.operation.commit.BoomiCommitter;
import com.boomi.connector.kafka.operation.consume.BoomiConsumer;
import com.boomi.connector.kafka.operation.consume.BoomiCustomConsumerSupplierFactory;
import com.boomi.connector.kafka.operation.produce.BoomiProducer;
import com.boomi.connector.kafka.util.Constants;
import com.boomi.connector.kafka.util.PartitionUtil;
import com.boomi.util.StringUtil;

import java.util.function.Supplier;

/**
 *
 */
public class KafkaOperationConnection extends KafkaConnection<OperationContext> {

    public KafkaOperationConnection(OperationContext context) {
        super(context);
    }

    public String getObjectTypeId() {
        return getContext().getObjectTypeId();
    }

    public BoomiProducer createProducer() {
        return new BoomiProducer(ProducerConfiguration.create(this));
    }

    /**
     * Create a {@link BoomiConsumer} and subscribe to the given topic. If {@link #isAssignPartitions()} is set, a
     * {@link BoomiConsumer} is returned with the assigned partitions.
     *
     * @param topic
     *         to subscribe
     * @return a new {@link BoomiConsumer}
     */
    public BoomiConsumer createConsumer(String topic) {
        Supplier<BoomiCustomConsumer> supplier = isAssignPartitions() ? createSupplier(topic, getPartitionsIds())
                : createSupplier(topic);
        return new BoomiConsumer(supplier);
    }

    /**
     * Create a {@link Supplier} of {@link BoomiCustomConsumer} with the given topic and partitions.
     *
     * @param topic
     *         name
     * @param partitions
     *         identifiers to be assigned to the given topic
     * @return a new {@link Supplier}
     */
    protected Supplier<BoomiCustomConsumer> createSupplier(String topic, String partitions) {
        return BoomiCustomConsumerSupplierFactory.createSupplier(ConsumerConfiguration.consumer(this),
                PartitionUtil.createTopicPartition(topic, partitions));
    }

    /**
     * Create a {@link Supplier} of {@link BoomiCustomConsumer} with the given topic.
     *
     * @param topic
     *         name
     * @return a new {@link Supplier}
     */
    protected Supplier<BoomiCustomConsumer> createSupplier(String topic) {
        return BoomiCustomConsumerSupplierFactory.createSupplier(ConsumerConfiguration.consumer(this), topic);
    }

    public BoomiCommitter createCommitter() {
        return new BoomiCommitter(ConsumerConfiguration.consumer(this));
    }

    @Override
    public String getClientId() {
        String clientId = getContext().getOperationProperties().getProperty(Constants.KEY_CLIENT_ID);

        if (StringUtil.isBlank(clientId)) {
            throw new ConnectorException("Client ID cannot be blank");
        }

        return clientId;
    }

    /**
     * Get the configured assign partitions.
     *
     * @return true if property is checked. Otherwise, false is returned.
     */
    protected boolean isAssignPartitions() {
        return getContext().getOperationProperties().getBooleanProperty(Constants.KEY_ASSIGN_PARTITIONS, false);
    }

    /**
     * Get the configured partitions ids.
     *
     * @return a String with the partition ids. Otherwise, empty is returned.
     */
    protected String getPartitionsIds() {
        return getContext().getOperationProperties().getProperty(Constants.KEY_PARTITION_IDS, StringUtil.EMPTY_STRING);
    }
}
