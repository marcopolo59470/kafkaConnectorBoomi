
package com.boomi.connector.kafka.operation.commit;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.kafka.operation.KafkaOperationConnection;
import com.boomi.connector.kafka.util.Constants;
import com.boomi.connector.kafka.util.ResultUtil;
import com.boomi.connector.kafka.util.TopicNameUtil;
import com.boomi.connector.util.BaseUpdateOperation;
import com.boomi.util.IOUtil;
import com.boomi.util.StringUtil;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Execute Operation for committing messages to an Apache Kafka Topic.
 * Commits messages to the Topic specified as Object Type or in the JSON input document
 */
public class CommitOffsetOperation extends BaseUpdateOperation {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().disable(
            MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS);

    private final String _topic;
    private final boolean _isDynamicTopic;

    public CommitOffsetOperation(KafkaOperationConnection connection) {
        super(connection);
        _topic = validateTopicIsNotBlank(TopicNameUtil.getTopic(connection.getObjectTypeId(), connection.getContext()));
        _isDynamicTopic = Constants.DYNAMIC_TOPIC_ID.equals(_topic);
    }

    private static String validateTopicIsNotBlank(String topic) {
        if (StringUtil.isBlank(topic)) {
            throw new ConnectorException("topic name cannot be blank");
        }

        return topic;
    }

    private static void commit(BoomiCommitter committer, CommitBatch batch, OperationResponse response) {
        try {
            CommitMessage message = batch.getMessageToCommit();
            committer.commit(message);
            ResultUtil.addSuccesses(batch.getDocuments(), response);
        } catch (KafkaException e) {
            ResultUtil.addApplicationErrors(batch.getDocuments(), response, Constants.CODE_ERROR, e.getMessage(), e);
        }
    }

    private CommitMessage parseData(ObjectData data) throws IOException {
        InputStream payload = null;

        try {
            payload = data.getData();
            CommitMessage message = OBJECT_MAPPER.readValue(payload, CommitMessage.class);
            if (_isDynamicTopic) {
                // for DYNAMIC TOPIC Object Type, the topic is obtained from the input document,
                // so it's necessary to validate it
                validateTopicIsNotBlank(message.getTopic());
            } else {
                // otherwise the topic is set from the selected Object Type
                message.updateTopic(_topic);
            }
            return message;
        } finally {
            IOUtil.closeQuietly(payload);
        }
    }

    private Collection<CommitBatch> buildBatches(Iterable<ObjectData> documents, OperationResponse response) {
        Map<TopicPartition, CommitBatch> batches = new HashMap<>();

        for (ObjectData document : documents) {
            CommitMessage message;
            try {
                message = parseData(document);
            } catch (Exception e) {
                String errorMessage = "Invalid input document: " + e.getMessage();
                ResultUtil.addApplicationError(document, response, Constants.CODE_ERROR, errorMessage, e);
                continue;
            }

            TopicPartition topic = message.getTopicPartition();
            batches.computeIfAbsent(topic, key -> new CommitBatch());
            CommitBatch batch = batches.get(topic);
            batch.update(message, document);
        }

        return batches.values();
    }

    @Override
    public KafkaOperationConnection getConnection() {
        return (KafkaOperationConnection) super.getConnection();
    }

    @Override
    protected void executeUpdate(UpdateRequest request, OperationResponse response) {
        BoomiCommitter committer = null;

        try {
            committer = getConnection().createCommitter();

            Collection<CommitBatch> batches = buildBatches(request, response);

            for (CommitBatch batch : batches) {
                commit(committer, batch, response);
            }
        } catch (Exception e) {
            ResponseUtil.addExceptionFailures(response, request, e);
        } finally {
            IOUtil.closeQuietly(committer);
        }
    }

}
