
package com.boomi.connector.kafka;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.kafka.client.consumer.BoomiCustomConsumer;
import com.boomi.connector.kafka.client.consumer.ConsumerConfiguration;
import com.boomi.connector.kafka.client.producer.BoomiCustomProducer;
import com.boomi.connector.kafka.client.producer.ProducerConfiguration;
import com.boomi.connector.kafka.configuration.KafkaConfiguration;
import com.boomi.connector.kafka.operation.CustomOperationType;
import com.boomi.connector.kafka.operation.KafkaOperationConnection;
import com.boomi.connector.kafka.operation.commit.BoomiCommitter;
import com.boomi.connector.kafka.util.Constants;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.boomi.util.ByteUnit;
import com.boomi.util.CollectionUtil;
import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;
import com.boomi.util.StringUtil;
import com.boomi.util.TestUtil;

import org.apache.kafka.clients.admin.BoomiAdminClient;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.annotation.Nonnull;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TestUtils {

    private static final Logger LOG = LogUtil.getLogger(TestUtils.class);

    private static final String COMMIT_OFFSET_MESSAGE_PATTERN_WITH_TOPIC =
            "{\"topic\": \"%s\", \"partition\": %s, \"offset\": %s, \"metadata\": \"%s\"}";
    private static final String COMMIT_OFFSET_MESSAGE_PATTERN_WITHOUT_TOPIC =
            "{\"partition\": %s, \"offset\": %s, \"metadata\": \"%s\"}";

    private static final String FIFTEEN_MEGABYTES = String.valueOf(ByteUnit.byteSize(15, ByteUnit.MB.name()));
    private static final Map<String, String> TOPIC_CONFIGS = CollectionUtil.mapBuilder("max.message.bytes",
            FIFTEEN_MEGABYTES).finishImmutable();

    private TestUtils() {
    }

    public static void disableLogs() {
        TestUtil.disableBoomiLog();
        TestUtil.disableLog("org.apache");
    }

    static KafkaConnection<BrowseContext> createConnection(BrowseContext context) {
        return new KafkaConnection<>(context);
    }

    public static KafkaOperationConnection createOperationConnection(OperationContext context) {
        return new KafkaOperationConnection(context);
    }

    public static KafkaOperationConnection createConnectionMock(OperationContext context, BoomiCommitter committer) {
        return new ConnectionMock(context, committer);
    }

    public static UpdateRequest stubRequest(final ObjectData... requests) {
        return new UpdateRequest() {
            final Iterator<ObjectData> iterator = Arrays.asList(requests).iterator();

            @Override
            @Nonnull
            public Iterator<ObjectData> iterator() {
                return iterator;
            }
        };
    }

    public static SimpleTrackedData createCommitDocument(int id, String topic, int partition, long offset,
            String metadata) {
        String json = String.format(COMMIT_OFFSET_MESSAGE_PATTERN_WITH_TOPIC, topic, partition, offset, metadata);
        return new SimpleTrackedData(id, new ByteArrayInputStream(json.getBytes(StringUtil.UTF8_CHARSET)));
    }

    public static SimpleTrackedData createCommitDocument(int id, int partition, long offset, String metadata) {
        String json = String.format(COMMIT_OFFSET_MESSAGE_PATTERN_WITHOUT_TOPIC, partition, offset, metadata);
        return new SimpleTrackedData(id, new ByteArrayInputStream(json.getBytes(StringUtil.UTF8_CHARSET)));
    }

    public static void createTopic(KafkaITContext context, String topic) {
        if (StringUtil.isEmpty(topic)) {
            throw new ConnectorException("topic name cannot be blank!");
        }

        NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
        newTopic.configs(TOPIC_CONFIGS);

        BoomiAdminClient client = null;
        try {
            client = getAdminClient(context);
            CreateTopicsResult result = client.createTopics(Collections.singleton(newTopic),
                    new CreateTopicsOptions().timeoutMs(100_000));
            result.all().get();
        } catch (ExecutionException e) {
            boolean isTopicExist = (e.getCause() instanceof TopicExistsException);
            // if topic already exist, ignore the error
            if (!isTopicExist) {
                throw new ConnectorException(e.getMessage(), e);
            }
        } catch (Exception e) {
            throw new ConnectorException(e.getMessage(), e);
        } finally {
            IOUtil.closeQuietly(client);
        }
    }

    public static void deleteTopic(KafkaITContext context, Collection<String> topics) {
        if (CollectionUtil.isEmpty(topics)) {
            return;
        }

        BoomiAdminClient client = null;
        try {
            client = getAdminClient(context);
            DeleteTopicsResult deleteTopicsResult = client.deleteTopics(topics);
            deleteTopicsResult.all().get();
        } catch (Exception e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
        } finally {
            IOUtil.closeQuietly(client);
        }
    }

    public static <K, V> void publishMessages(KafkaITContext context, Collection<ProducerRecord<K, V>> records) {
        if (CollectionUtil.isEmpty(records)) {
            return;
        }

        BoomiCustomProducer<K, V> producer = null;
        try {
            producer = new BoomiCustomProducer<>(new TestProduceConfiguration(createOperationConnection(context)));

            for (ProducerRecord<K, V> record : records) {
                Future<RecordMetadata> send = producer.send(record);
                producer.flush();
                send.get(10L, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            throw new ConnectorException(e.getMessage(), e);
        } finally {
            IOUtil.closeQuietly(producer);
        }
    }

    public static Collection<ConsumerRecord<String, InputStream>> consumeMessages(KafkaITContext context,
            String topic) {
        Collection<ConsumerRecord<String, InputStream>> records = new ArrayList<>();
        BoomiCustomConsumer consumer = null;

        long timeout = System.currentTimeMillis() + 10_000L;

        try {
            consumer = new BoomiCustomConsumer(new TestConsumeConfiguration(createOperationConnection(context)));
            consumer.subscribe(topic);

            do {
                ConsumerRecords<String, InputStream> poll = consumer.poll(100L);
                for (ConsumerRecord<String, InputStream> record : poll) {
                    records.add(record);
                }
            } while (records.size() < 1 && System.currentTimeMillis() < timeout);

            return records;
        } catch (Exception e) {
            throw new ConnectorException(e.getMessage(), e);
        } finally {
            IOUtil.closeQuietly(consumer);
        }
    }

    private static BoomiAdminClient getAdminClient(KafkaITContext context) {
        CustomOperationType type = CustomOperationType.fromContext(context);
        KafkaConfiguration<?> configuration;

        KafkaOperationConnection connection = createOperationConnection(context);

        switch (type) {
            case PRODUCE:
                configuration = ProducerConfiguration.create(connection);
                break;
            case COMMIT_OFFSET:
            case CONSUME:
                context.addOperationProperty(Constants.KEY_CONSUMER_GROUP, "integrationtestconsumergroup");
                context.addOperationProperty(Constants.KEY_AUTO_OFFSET_RESET, "earliest");
                configuration = ConsumerConfiguration.consumer(connection);
                break;
            default:
                throw new UnsupportedOperationException();
        }

        return BoomiAdminClient.create(configuration);
    }

    private static class TestProduceConfiguration extends ProducerConfiguration {

        TestProduceConfiguration(KafkaOperationConnection connection) {
            super(connection);
        }

        @Override
        public ProducerConfig getConfig() {
            Map<String, Object> configs = new HashMap<>(getConfigs());
            configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            configs.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, FIFTEEN_MEGABYTES);
            return new ProducerConfig(configs);
        }
    }

    private static class TestConsumeConfiguration extends ConsumerConfiguration {

        TestConsumeConfiguration(KafkaOperationConnection connection) {
            super(connection);
        }

        @Override
        public ConsumerConfig getConfig() {
            Map<String, Object> configs = new HashMap<>(getConfigs());
            configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            return new ConsumerConfig(configs);
        }
    }

    private static class ConnectionMock extends KafkaOperationConnection {

        private final BoomiCommitter _committer;

        ConnectionMock(OperationContext context, BoomiCommitter committer) {
            super(context);
            _committer = committer;
        }

        @Override
        public BoomiCommitter createCommitter() {
            return _committer;
        }
    }
}
