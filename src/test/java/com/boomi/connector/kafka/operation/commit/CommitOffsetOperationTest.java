
package com.boomi.connector.kafka.operation.commit;

import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.kafka.KafkaITContext;
import com.boomi.connector.kafka.TestUtils;
import com.boomi.connector.kafka.operation.KafkaOperationConnection;
import com.boomi.connector.kafka.util.Constants;
import com.boomi.connector.testutil.ResponseFactory;
import com.boomi.connector.testutil.SimpleOperationResponse;
import com.boomi.connector.testutil.SimpleOperationResult;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.boomi.util.StringUtil;
import com.boomi.util.io.FastByteArrayInputStream;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.util.List;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(Theories.class)
public class CommitOffsetOperationTest {

    static {
        TestUtils.disableLogs();
    }

    @Test
    public void wrongConfigurationTest() {
        KafkaITContext context = KafkaITContext.commitOffsetVM();
        context.addOperationProperty(Constants.KEY_CONSUMER_GROUP, StringUtil.EMPTY_STRING);
        context.setObjectTypeId(Constants.DYNAMIC_TOPIC_ID);

        SimpleTrackedData data = new SimpleTrackedData(0, null);
        SimpleOperationResponse response = ResponseFactory.get(data);
        UpdateRequest request = TestUtils.stubRequest(data);

        CommitOffsetOperation operation = new CommitOffsetOperation(TestUtils.createOperationConnection(context));

        // execute sut
        operation.executeUpdate(request, response);

        // asserts
        List<SimpleOperationResult> results = response.getResults();

        Assert.assertEquals(1, results.size());
        for (SimpleOperationResult result : results) {
            Assert.assertEquals(OperationStatus.FAILURE, result.getStatus());
        }
    }

    @DataPoints("payloadWithoutField")
    public static String[] payloadsWithoutAField() {
        String withoutOffset =  "{\"topic\":\"TestTopic\",\"partition\":1,\"metadata\":\"Committed by Boomi Connector\"}";
        String withoutPartition =  "{\"topic\":\"TestTopic\",\"offset\":1,\"metadata\":\"Committed by Boomi Connector\"}";

        return new String[]{withoutOffset, withoutPartition};
    }

    @Theory
    public void shouldFailPayloadWithoutField(@FromDataPoints("payloadWithoutField") String payload) {
        KafkaITContext context = KafkaITContext.commitOffsetVM();
        context.setObjectTypeId(Constants.DYNAMIC_TOPIC_ID);
        BoomiCommitter committerMock = Mockito.mock(BoomiCommitter.class);
        KafkaOperationConnection connectionMock = TestUtils.createConnectionMock(context, committerMock);

        SimpleTrackedData data = new SimpleTrackedData(0,
                new FastByteArrayInputStream(payload.getBytes(StringUtil.UTF8_CHARSET)));

        UpdateRequest request = TestUtils.stubRequest(data);
        SimpleOperationResponse response = ResponseFactory.get(data);

        // execute
        CommitOffsetOperation operation = new CommitOffsetOperation(connectionMock);
        operation.executeUpdate(request, response);

        // assert results
        List<SimpleOperationResult> results = response.getResults();
        for (SimpleOperationResult result : results) {
            Assert.assertEquals(OperationStatus.APPLICATION_ERROR, result.getStatus());
            Assert.assertEquals(Constants.CODE_ERROR, result.getStatusCode());
        }

        // commit shouldn't be called any time
        verify(committerMock, times(0)).commit(any(CommitMessage.class));
    }

    @Test
    public void shouldCallCommitOncePerTopicAndPartitionTest() {
        KafkaITContext context = KafkaITContext.commitOffsetVM();
        context.setObjectTypeId(Constants.DYNAMIC_TOPIC_ID);
        BoomiCommitter committerMock = Mockito.mock(BoomiCommitter.class);
        KafkaOperationConnection connectionMock = TestUtils.createConnectionMock(context, committerMock);

        final int partition1 = 1;
        final int partition2 = 2;

        final String topic1 = "topic1";
        SimpleTrackedData data1 = TestUtils.createCommitDocument(0, topic1, partition1, 1L, StringUtil.EMPTY_STRING);
        SimpleTrackedData data2 = TestUtils.createCommitDocument(1, topic1, partition2, 10L, StringUtil.EMPTY_STRING);
        SimpleTrackedData data3 = TestUtils.createCommitDocument(2, topic1, partition1, 2L, StringUtil.EMPTY_STRING);

        final long lastOffsetT1P1 = 2L;
        final long lastOffsetT1P2 = 10L;

        final String topic2 = "topic2";
        SimpleTrackedData data4 = TestUtils.createCommitDocument(3, topic2, partition2, 3L, StringUtil.EMPTY_STRING);
        SimpleTrackedData data5 = TestUtils.createCommitDocument(4, topic2, partition1, 5L, StringUtil.EMPTY_STRING);
        SimpleTrackedData data6 = TestUtils.createCommitDocument(5, topic2, partition2, 7L, StringUtil.EMPTY_STRING);

        final long lastOffsetT2P1 = 5L;
        final long lastOffsetT2P2 = 7L;

        UpdateRequest request = TestUtils.stubRequest(data1, data4, data2, data5, data3, data6);
        SimpleOperationResponse response = ResponseFactory.get(data1, data4, data2, data5, data3, data6);

        // execute
        CommitOffsetOperation operation = new CommitOffsetOperation(connectionMock);
        operation.executeUpdate(request, response);

        // assert results
        List<SimpleOperationResult> results = response.getResults();
        for (SimpleOperationResult result : results) {
            Assert.assertEquals(OperationStatus.SUCCESS, result.getStatus());
            Assert.assertEquals(Constants.CODE_SUCCESS, result.getMessage());
        }

        CommitMessage messageT1P1 = new CommitMessage(topic1, partition1, lastOffsetT1P1);
        CommitMessage messageT1P2 = new CommitMessage(topic1, partition2, lastOffsetT1P2);
        CommitMessage messageT2P1 = new CommitMessage(topic2, partition1, lastOffsetT2P1);
        CommitMessage messageT2P2 = new CommitMessage(topic2, partition2, lastOffsetT2P2);

        final int once = 1;

        verify(committerMock, times(once)).commit(eq(messageT1P1));
        verify(committerMock, times(once)).commit(eq(messageT1P2));
        verify(committerMock, times(once)).commit(eq(messageT2P1));
        verify(committerMock, times(once)).commit(eq(messageT2P2));

        final int fourTimes = 4;
        // commit called four times in total
        verify(committerMock, times(fourTimes)).commit(any(CommitMessage.class));
    }
}
