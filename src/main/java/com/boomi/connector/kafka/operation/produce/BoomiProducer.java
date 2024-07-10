
package com.boomi.connector.kafka.operation.produce;

import com.boomi.connector.kafka.client.producer.BoomiCustomProducer;
import com.boomi.connector.kafka.client.producer.ProducerConfiguration;
import com.boomi.util.IOUtil;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Closeable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Facade for {@link BoomiCustomProducer} used to expose only the methods required by {@link ProduceOperation} in order
 * to get messages from Kafka.
 */
public class BoomiProducer implements Closeable {

    private final BoomiCustomProducer<String, MessagePayload> _producer;

    public BoomiProducer(ProducerConfiguration boomiConfiguration) {
        _producer = new BoomiCustomProducer<>(boomiConfiguration);
    }

    /**
     * Sends the given message to the Service.
     *
     * @param record
     *         The message to be sent.
     * @throws ExecutionException
     *         If the service returns any error after sending the message.
     * @throws InterruptedException
     *         If the thread is interrupted while blocked.
     * @throws TimeoutException
     *         If the time taken for sending the message has surpassed the maximum wait timeout configured in the
     *         producer.
     */
    void sendMessage(ProducerRecord<String, MessagePayload> record)
            throws InterruptedException, ExecutionException, TimeoutException {
        _producer.sendMessage(record);
    }

    /**
     * @return the maximum message size allow for the publish operation. This value is set as a container property.
     */
    int getMaxRequestSize() {
        return _producer.getMaxRequestSize();
    }

    @Override
    public void close() {
        IOUtil.closeQuietly(_producer);
    }
}
