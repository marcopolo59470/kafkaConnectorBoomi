package com.boomi.connector.kafka.operation.produce;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.kafka.client.producer.BoomiCustomProducer;
import com.boomi.connector.kafka.client.producer.ProducerConfiguration;
import com.boomi.util.IOUtil;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Closeable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Facade for {@link BoomiCustomProducer} used to expose only the methods required by {@link ProduceOperation} in order
 * to send messages to Kafka. This class is generalized to handle any type of key and value.
 */
public class BoomiProducer<K, V> implements Closeable {

    private final BoomiCustomProducer<K, V> producer;

    public BoomiProducer(ProducerConfiguration boomiConfiguration) {
        this.producer = new BoomiCustomProducer<>(boomiConfiguration);
    }

    /**
     * Sends the given message to the Service.
     *
     * @param record The message to be sent.
     * @throws ExecutionException If the service returns any error after sending the message.
     * @throws InterruptedException If the thread is interrupted while blocked.
     * @throws TimeoutException If the time taken for sending the message has surpassed the maximum wait timeout configured in the producer.
     */
    public void sendMessage(ProducerRecord<K, V> record)
            throws InterruptedException, ExecutionException, TimeoutException {
        producer.sendMessage(record);
    }

    /**
     * @return the maximum message size allow for the publish operation. This value is set as a container property.
     */
    public int getMaxRequestSize() {
        return producer.getMaxRequestSize();
    }

    @Override
    public void close() {
        IOUtil.closeQuietly(producer);
    }
}
