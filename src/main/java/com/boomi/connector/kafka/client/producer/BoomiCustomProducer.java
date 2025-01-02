package com.boomi.connector.kafka.client.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Custom Kafka Producer wrapper for additional functionalities.
 */
public class BoomiCustomProducer<K, V> implements AutoCloseable {

    private final KafkaProducer<K, V> kafkaProducer;
    private final long maxWaitTimeout;
    private final int maxRequestSize;

    /**
     * Constructor to initialize KafkaProducer with custom configuration.
     *
     * @param configuration the producer configuration.
     */
    public BoomiCustomProducer(ProducerConfiguration configuration) {
        Properties props = new Properties();
        props.putAll(configuration.getConfig().originals());

        this.kafkaProducer = new KafkaProducer<>(props);
        this.maxWaitTimeout = configuration.getMaxWaitTimeout();
        this.maxRequestSize = configuration.getMaxRequestSize();
    }

    /**
     * Synchronously send a record to a topic. This method blocks until the message is sent or the configured timeout
     * is reached.
     *
     * @param record the message to be sent.
     * @throws ExecutionException   If the service returns any error after sending the message.
     * @throws InterruptedException If the thread is interrupted while blocked.
     * @throws TimeoutException     If the time taken for sending the message has surpassed the maximum wait timeout
     *                               configured in the producer.
     */
    public void sendMessage(ProducerRecord<K, V> record)
            throws InterruptedException, ExecutionException, TimeoutException {
        Future<RecordMetadata> sendFuture = kafkaProducer.send(record);
        kafkaProducer.flush(); // Ensure all previous records are sent before proceeding.
        sendFuture.get(maxWaitTimeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Get the maximum request size configured for this producer.
     *
     * @return the maximum request size.
     */
    public int getMaxRequestSize() {
        return maxRequestSize;
    }

    /**
     * Closes the producer and releases any resources held by it.
     */
    @Override
    public void close() {
        if (kafkaProducer != null) {
            kafkaProducer.close();
        }
    }
}
