package com.boomi.connector.kafka.operation.produce;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.DynamicPropertyMap;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.kafka.exception.InvalidMessageSizeException;
import com.boomi.connector.kafka.util.AvroMapper;
import com.boomi.connector.kafka.util.Constants;
import com.boomi.util.CollectionUtil;
import com.boomi.util.IOUtil;
import com.boomi.util.StringUtil;

import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.boomi.connector.kafka.util.AvroMapper.convertInputStreamToString;
import static com.boomi.connector.kafka.util.Tools.translateEscapes;

/**
 * Representation of the messages to publish to Apache Kafka service.
 */
public class ProduceMessage implements Closeable {

    private static final String ERROR_MESSAGE_SIZE_INVALID =
            "The message size {0} is larger than the maximum expected {1}";
    private final String _key;
    private final MessagePayload _payload;
    private final String _topic;
    private final Iterable<Header> _headers;
    private final Integer _partitionId;

    /**
     * Construct a new instance of {@link ProduceMessage} from the given {@link ObjectData}.
     *
     * Call {@link ProduceMessage#toRecord()} to get the {@link ProducerRecord} associated with this instance.
     *
     * @param data
     *         to extract
     * @param topic
     *         where this message will be produced
     * @param maxAllowedSize
     *         for the message payload
     * @throws InvalidMessageSizeException
     *         if the message payload is larger than {@param maxAllowedSize}
     */
    ProduceMessage(ObjectData data, String topic, int maxAllowedSize) throws InvalidMessageSizeException {
        _key = extractKey(data);
        _topic = getTopic(data, topic);
        int size = getSize(data, maxAllowedSize);
        _payload = new MessagePayload(data.getData(), size);
        DynamicPropertyMap operationProperties = data.getDynamicOperationProperties();
        _headers = getHeaderProperties(operationProperties.getCustomProperties(Constants.HEADER_PROPERTIES_KEY));
        _partitionId = getPartitionId(operationProperties);
    }

    /**
     * Extracts the operation property partition_id to send to Apache Kafka as partition id, If the property is {@code
     * null} or blank, a {@code null} value is returned, letting Kafka assign a specific partition.
     *
     * @param operationProperties
     *         to get partition property
     * @return the partition id
     */
    private static Integer getPartitionId(DynamicPropertyMap operationProperties) {
        String partitionId = operationProperties.getProperty(Constants.KEY_PARTITION_ID);
        if (StringUtil.isBlank(partitionId)) {
            return null;
        }
        try {
            return Integer.parseInt(partitionId);
        } catch (NumberFormatException e) {
            throw new ConnectorException("Partition id must be a number.", e);
        }
    }

    private static List<Header> getHeaderProperties(Map<String, String> headerProperties) {
        if (CollectionUtil.isEmpty(headerProperties)) {
            return Collections.emptyList();
        }
        return headerProperties.entrySet().stream().map(
                        header -> new RecordHeader(header.getKey(), header.getValue().getBytes(StandardCharsets.UTF_8)))
                .collect(Collectors.toList());
    }

    /**
     * If message size is available and is lower than the maximum allowed, it returns the message size. Else throws
     * InvalidMessageSizeException.
     *
     * @param data
     *         The message to send to Apache Kafka service.
     * @param maxAllowedSize
     *         The maximum allowed size of the message to send.
     * @return the message size.
     * @throws InvalidMessageSizeException
     *         If message size is not available or higher than maximum allowed size.
     */
    private static int getSize(ObjectData data, int maxAllowedSize) throws InvalidMessageSizeException {
        try {
            long size = data.getDataSize();
            if (size <= maxAllowedSize) {
                return (int) size;
            }
            throw new InvalidMessageSizeException(
                    MessageFormat.format(ERROR_MESSAGE_SIZE_INVALID, size, maxAllowedSize));
        } catch (IOException e) {
            throw new InvalidMessageSizeException(Constants.ERROR_MESSAGE_SIZE_NOT_AVAILABLE, e);
        }
    }

    /**
     * Extracts the dynamic property message_key from the message to send to Apache Kafka service.
     *
     * @param data
     *         The message to send to Apache Kafka service.
     * @return the message key.
     */
    private static String extractKey(ObjectData data) {
        return translateEscapes(data.getDynamicProperties().get(Constants.KEY_MESSAGE_KEY));
    }

    /**
     * If the topic is selected in the operation it returns the object type id, else if the topic is set as a dynamic
     * property it returns the dynamic property value, overriding the one selected in the operation. If the topic is set
     * as a dynamic property and the value is null or empty it throws ConnectorException.
     *
     * @param data
     *         The message to send to Apache Kafka service.
     * @param topic
     *         The topic name selected in the operation or "DYNAMIC_TOPIC".
     * @return the message topic.
     * @throws ConnectorException
     *         If the topic is set as a dynamic property and value is null or empty.
     */
    private static String getTopic(ObjectData data, String topic) {
        boolean isDynamicTopic = Constants.DYNAMIC_TOPIC_ID.equals(topic);

        if (!isDynamicTopic) {
            return topic;
        }

        String overriddenTopic = data.getDynamicProperties().get(Constants.KEY_TOPIC_NAME);

        if (StringUtil.isBlank(overriddenTopic)) {
            throw new ConnectorException("undefined topic");
        }

        return overriddenTopic;
    }

    /**
     * Gets a message record representation to send to the service.
     *
     * @return a message record to send to Apache Kafka service.
     */
    ProducerRecord<String, MessagePayload> toRecord() {
        return new ProducerRecord<>(_topic, _partitionId, _key, _payload, _headers);
    }

    ProducerRecord<String, GenericData.Record> toAvroMessageRecord(String schemaMessage) throws IOException {
        AvroMapper avroMessageMapper = new AvroMapper(translateEscapes(schemaMessage));
        String messageString = convertInputStreamToString(_payload.getInputStream());
        GenericData.Record message = avroMessageMapper.toAvroRecord(translateEscapes(messageString));
        return new ProducerRecord<>(_topic, _partitionId, _key, message, _headers);
    }

    ProducerRecord<GenericData.Record, GenericData.Record> toAvroMessageAndKeyRecord(String schemaKey, String schemaMessage) throws IOException {
        AvroMapper avroKeyMapper = new AvroMapper(translateEscapes(schemaKey));
        AvroMapper avroMessageMapper = new AvroMapper(translateEscapes(schemaMessage));
        String messageString = convertInputStreamToString(_payload.getInputStream());
        GenericData.Record key = avroKeyMapper.toAvroRecord(_key);

        GenericData.Record message = avroMessageMapper.toAvroRecord(messageString);
        //throw new ConnectorException("p/ " + message + "   k/ " + key);
        return new ProducerRecord<>(_topic, _partitionId, key, message, _headers);
    }

    /**
     * Closes ProduceMessage's MessagePayload.
     */
    @Override
    public void close() {
        IOUtil.closeQuietly(_payload);
    }
}
