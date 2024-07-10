
package com.boomi.connector.kafka.operation.consume;

import com.boomi.connector.kafka.client.consumer.BoomiCustomConsumer;
import com.boomi.connector.kafka.client.consumer.ConsumeMessage;
import com.boomi.connector.kafka.exception.CommitOffsetException;
import com.boomi.connector.kafka.operation.commit.Committable;
import com.boomi.util.CollectionUtil;
import com.boomi.util.IOUtil;

import org.apache.kafka.common.TopicPartition;

import java.io.Closeable;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Façade for {@link BoomiCustomConsumer} used to expose only the methods required by {@link ConsumeOperation} in order
 * to get messages from Kafka.
 */
public class BoomiConsumer implements Closeable {

    private BoomiCustomConsumer _consumer;
    private final Supplier<BoomiCustomConsumer> _consumerSupplier;

    public BoomiConsumer(Supplier<BoomiCustomConsumer> consumerSupplier) {
        _consumerSupplier = consumerSupplier;
        _consumer = consumerSupplier.get();
    }

    /**
     * Fetches a batch of messages from the subscribed topic.
     *
     * @return a batch of messages.
     */
    public Iterable<ConsumeMessage> poll(long timeout) {
        return _consumer.pollMessages(timeout);
    }

    /**
     * Commit the given {@link Committable}.
     *
     * @param message
     *         the message to commit.
     */
    void commit(Committable message) throws CommitOffsetException {
        try {
            _consumer.commit(message);
        } catch (Exception e) {
            throw new CommitOffsetException(e);
        }
    }

    /**
     * Build a {@link ConsumeMessage} from the metadata of the last message retrieved if the consumer is subscribed to a
     * single partition, null otherwise.
     *
     * @return the Consume Message
     */
    ConsumeMessage getLastErrorMessage() {
        Set<TopicPartition> assignment = _consumer.assignment();
        TopicPartition partition = (assignment.size() == 1) ? CollectionUtil.getFirst(assignment) : null;
        return (partition == null) ? null : new ConsumeMessage(partition, _consumer.position(partition));
    }

    void reSubscribe() {
        IOUtil.closeQuietly(_consumer);
        _consumer = _consumerSupplier.get();
    }

    @Override
    public void close() {
        IOUtil.closeQuietly(_consumer);
    }
}
