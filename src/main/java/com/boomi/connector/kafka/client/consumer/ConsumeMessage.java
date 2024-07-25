package com.boomi.connector.kafka.client.consumer;

import com.boomi.connector.api.Payload;
import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.api.PayloadUtil;
import com.boomi.connector.kafka.operation.commit.Committable;
import com.boomi.connector.kafka.operation.polling.BoomiListenerConsumer;
import com.boomi.connector.kafka.util.Constants;
import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;
import com.boomi.util.StreamUtil;
import com.boomi.util.StringUtil;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Representation of the messages consumed from Kafka
 */
public class ConsumeMessage implements Committable, Closeable {

    private static final String CUSTOM_HEADER_PROPERTIES_GROUP_ID = "custom_header_properties";
    private final long _offset;
    private final Object _key; // Changed from String to Object to handle any type of key
    private final TopicPartition _topicPartition;
    private final InputStream _message;
    private final Headers _headers;
    private final Long _timestamp;

    private static final Logger LOG = LogUtil.getLogger(ConsumeMessage.class);

    /**
     * Construct a new instance of {@link ConsumeMessage} from the attributes of the given {@link ConsumerRecord}.
     *
     * @param record containing the message attributes
     */
    /**public ConsumeMessage(ConsumerRecord<?, Object> record) {
        this(new TopicPartition(record.topic(), record.partition()), record.offset(), record.key(), record.value(),
                record.headers(), record.timestamp());
    }*/
    public ConsumeMessage(ConsumerRecord<?, InputStream> record) {
        this._topicPartition = new TopicPartition(record.topic(), record.partition());
        this._offset = record.offset();
        this._key = record.key();
        this._headers = record.headers();
        this._timestamp = record.timestamp();
        this._message = convertValueToInputStream(record.value());

    }

    private InputStream convertValueToInputStream(Object value) {
        if (value instanceof InputStream) {
            return (InputStream) value;
        } else if (value instanceof GenericRecord) {
            //return convertGenericRecordToInputStream((GenericRecord) value);
            String json = convertGenericRecordToJson((GenericRecord) value);
            return convertStringToInputStream(json);
        } else {
            throw new IllegalArgumentException("Unsupported value type: " + value.getClass().getName());
        }
    }

    private InputStream convertGenericRecordToInputStream(GenericRecord record) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(record.getSchema());
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        try {
            datumWriter.write(record, encoder);
            encoder.flush();
            return new ByteArrayInputStream(outputStream.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize GenericRecord", e);
        } finally {
            IOUtil.closeQuietly(outputStream);
        }
    }

    private String convertGenericRecordToJson(GenericRecord record) {
        try {
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            JsonEncoder encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), stream, true);
            writer.write(record, encoder);
            encoder.flush();
            return stream.toString(StandardCharsets.UTF_8.name());
        } catch (IOException e) {
            throw new RuntimeException("Failed to convert GenericRecord to JSON", e);
        }
    }

    private InputStream convertStringToInputStream(String data) {
        return new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
    }




    /**
     * This constructor is used to create a Message without payload that only holds the metadata associated with the
     * topic, partition and offset
     *
     * @param topicPartition containing the topic name and partition number
     * @param offset the position of the message in the partition
     */
    public ConsumeMessage(TopicPartition topicPartition, long offset) {
        this(topicPartition, offset, null, StreamUtil.EMPTY_STREAM, new RecordHeaders(), null);
    }

    private ConsumeMessage(TopicPartition topicPartition, long offset, Object key, InputStream message,
                           Headers headers, Long timestamp) {
        _topicPartition = topicPartition;
        _offset = offset;
        _key = key; // No casting here, direct assignment
        _message = message;
        _headers = headers;
        _timestamp = timestamp;
    }

    /**
     * Builds a {@link Payload} containing the message as input and its metadata as Tracked Properties
     *
     * @return the payload
     */
    public Payload toPayload(PayloadMetadata metadata) {
        metadata.setTrackedProperty(Constants.KEY_TOPIC_NAME, _topicPartition.topic());
        metadata.setTrackedProperty(Constants.KEY_MESSAGE_OFFSET, String.valueOf(_offset));
        metadata.setTrackedProperty(Constants.KEY_TOPIC_PARTITION, String.valueOf(_topicPartition.partition()));

        // Handling the key based on its type
        String keyString;

        if (_key instanceof GenericRecord) {
            // Si la clé est un GenericRecord, utiliser toString pour obtenir sa représentation sous forme de chaîne.
            keyString = _key.toString();
        } else if (_key instanceof String) {
            // Si la clé est déjà une String, pas besoin de conversion.
            keyString = (String) _key;
        } else if (_key != null) {
            // Si la clé est d'un autre type, utiliser toString pour éviter des erreurs de cast.
            keyString = _key.toString();
            // Vous pouvez aussi logger cette situation pour en comprendre la fréquence et les types impliqués.
            // Log.warning("Unexpected key type: " + _key.getClass().getName());
        } else {
            // Si la clé est nulle, vous pouvez assigner une valeur par défaut ou gérer ce cas spécifique.
            keyString = "null";
        }

// Utiliser keyString pour la suite des opérations...

        metadata.setTrackedProperty(Constants.KEY_MESSAGE_KEY, keyString);

        String timestamp = _timestamp == null ? StringUtil.EMPTY_STRING : String.valueOf(_timestamp);
        metadata.setTrackedProperty(Constants.KEY_MESSAGE_TIMESTAMP, timestamp);

        Map<String, String> propertyGroup = new HashMap<>();
        for (Header header : _headers) {
            propertyGroup.put(header.key(), new String(header.value(), StringUtil.UTF8_CHARSET));
        }
        metadata.setTrackedGroupProperties(CUSTOM_HEADER_PROPERTIES_GROUP_ID, propertyGroup);

        return PayloadUtil.toPayload(_message, metadata);
    }

    /**
     * @return the {@link TopicPartition} where this message come from
     */
    @Override
    public TopicPartition getTopicPartition() {
        return _topicPartition;
    }

    /**
     * @return the offset position to the next message
     */
    @Override
    public OffsetAndMetadata getNextOffset() {
        return new OffsetAndMetadata(_offset + 1);
    }

    /**
     * @return the offset position of this message
     */
    public OffsetAndMetadata getOffset() {
        return new OffsetAndMetadata(_offset);
    }

    @Override
    public void close() {
        IOUtil.closeQuietly(_message);
    }
}
