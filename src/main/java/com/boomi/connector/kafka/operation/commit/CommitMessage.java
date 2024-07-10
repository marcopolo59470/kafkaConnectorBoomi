
package com.boomi.connector.kafka.operation.commit;

import com.boomi.connector.api.ConnectorException;
import com.boomi.util.EqualsBuilder;
import com.boomi.util.HashCodeBuilder;
import com.boomi.util.StringUtil;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * Representation of the message to be committed. This class is intended to be instantiated using Jackson from the
 * Input Document.
 */
public class CommitMessage implements Committable {

    private final int _partition;
    private final long _offset;
    private final String _metadata;
    private String _topic;

    @JsonCreator
    public CommitMessage(@JsonProperty(value = "topic") String topic,
            @JsonProperty(value = "partition") Integer partition, @JsonProperty(value = "offset") Long offset,
            @JsonProperty(value = "metadata") String metadata) {

        if (partition == null) {
            throw new ConnectorException("partition field must be present");
        }
        _partition = partition;

        if (offset == null) {
            throw new ConnectorException("offset field must be present");
        }
        _offset = offset;

        _topic = topic;
        _metadata = metadata;
    }

    // for testing purposes
    CommitMessage(String topic, int partition, long offset) {
        this(topic, partition, offset, StringUtil.EMPTY_STRING);
    }

    String getTopic() {
        return _topic;
    }

    void updateTopic(String topic) {
        _topic = topic;
    }

    /**
     * @return the {@link TopicPartition} where this message came from
     */
    @Override
    public TopicPartition getTopicPartition() {
        return new TopicPartition(_topic, _partition);
    }

    /**
     * @return the offset for the next message in the topic
     */
    @Override
    public OffsetAndMetadata getNextOffset() {
        long nextOffset = _offset + 1;
        return new OffsetAndMetadata(nextOffset, _metadata);
    }

    boolean isBefore(CommitMessage other) {
        if (!_topic.equals(other._topic)) {
            throw new ConnectorException("cannot compare messages from different topic");
        }

        if (_partition != other._partition) {
            throw new ConnectorException("cannot compare messages from different partition");
        }

        return (_offset <= other._offset);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(41, 73).append(_topic).append(_partition).append(_offset).append(_metadata)
                .toHashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof CommitMessage)) {
            return false;
        }

        CommitMessage other = (CommitMessage) o;
        return new EqualsBuilder().append(other._topic, _topic).append(other._metadata, _metadata).append(other._offset,
                _offset).append(other._partition, _partition).isEquals();
    }
}
