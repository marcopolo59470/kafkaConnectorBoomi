package com.boomi.connector.kafka.client.producer;

import com.boomi.connector.api.ConnectorException;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;
import org.apache.kafka.clients.producer.internals.ProducerMetadata;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Time;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * An extension of {@link KafkaProducer} with custom method overloads.
 */
public class BoomiCustomProducer<K, V> extends KafkaProducer<K, V> {

    private final long _maxWaitTimeout;
    private int _maxRequestSize;

    public BoomiCustomProducer(ProducerConfiguration configuration) {
        super(configuration.getConfig(), configuration.getClientId(), configuration.getChannelBuilder(),
                configuration.getMaxRequestSize());
        _maxWaitTimeout = configuration.getMaxWaitTimeout();
        _maxRequestSize = configuration.getMaxRequestSize();

    }

    /**
     * Synchronously send a record to a topic. This method blocks until the message is sent or the configured timeout
     * is reached.
     *
     * @param record
     *         the message to be sent
     * @throws ExecutionException
     *         If the service returns any error after sending the message.
     * @throws InterruptedException
     *         If the thread is interrupted while blocked.
     * @throws TimeoutException
     *         If the time taken for sending the message has surpassed the maximum wait timeout configured in the
     *         producer.
     */
    public void sendMessage(ProducerRecord<K, V> record)
            throws InterruptedException, ExecutionException, java.util.concurrent.TimeoutException {
        Future<RecordMetadata> send = send(record);
        flush();//
        send.get(_maxWaitTimeout, TimeUnit.MILLISECONDS);
    }

    public int getMaxRequestSize() {
        return _maxRequestSize;
    }
}
