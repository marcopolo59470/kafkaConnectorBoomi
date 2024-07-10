
package com.boomi.connector.kafka.operation.commit;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.testutil.SimpleTrackedData;

import org.junit.Assert;
import org.junit.Test;

public class CommitBatchTest {

    @Test
    public void updateWithLargerOffsetTest() {
        final int partition = 0;
        final String topic = "topicForTest";
        CommitMessage messageWithOffsetZero = new CommitMessage(topic, partition, 0L);
        SimpleTrackedData document1 = new SimpleTrackedData(0, null);

        CommitBatch batch = new CommitBatch();
        batch.update(messageWithOffsetZero, document1);

        CommitMessage messageWithOffsetTen = new CommitMessage(topic, partition, 10L);
        SimpleTrackedData document2 = new SimpleTrackedData(1, null);

        batch.update(messageWithOffsetTen, document2);

        Assert.assertEquals(2, batch.getDocuments().size());

        CommitMessage message = batch.getMessageToCommit();
        Assert.assertEquals(messageWithOffsetTen, message);
    }

    @Test
    public void updateWithSmallerOffsetTest() {
        final int partition = 0;
        final String topic = "topicForTest";
        CommitMessage messageWithOffsetTen = new CommitMessage(topic, partition, 10L);

        CommitBatch batch = new CommitBatch();
        batch.update(messageWithOffsetTen, new SimpleTrackedData(0, null));

        CommitMessage messageWithOffsetZero = new CommitMessage(topic, partition, 0L);
        SimpleTrackedData document2 = new SimpleTrackedData(1, null);

        batch.update(messageWithOffsetZero, document2);

        Assert.assertEquals(2, batch.getDocuments().size());

        CommitMessage message = batch.getMessageToCommit();
        Assert.assertEquals(messageWithOffsetTen, message);
    }

    @Test(expected = ConnectorException.class)
    public void updateWithDifferentPartitions() {
        final String topic = "topicForTest";
        CommitMessage messageWithPartitionOne = new CommitMessage(topic, 1, 10L);
        CommitMessage messageWithPartitionTwo = new CommitMessage(topic, 2, 11L);

        CommitBatch batch = new CommitBatch();

        batch.update(messageWithPartitionOne, new SimpleTrackedData(0, null));
        batch.update(messageWithPartitionTwo, new SimpleTrackedData(1, null));
    }

    @Test(expected = ConnectorException.class)
    public void updateWithDifferentTopics() {
        CommitMessage messageWithPartitionOne = new CommitMessage("topic1", 1, 10L);
        CommitMessage messageWithPartitionTwo = new CommitMessage("topic2", 2, 11L);

        CommitBatch batch = new CommitBatch();

        batch.update(messageWithPartitionOne, new SimpleTrackedData(0, null));
        batch.update(messageWithPartitionTwo, new SimpleTrackedData(1, null));
    }
}
