
package com.boomi.connector.kafka.operation.commit;

import com.boomi.connector.api.ConnectorException;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CommitMessageTest {

    @Test
    public void commitMessageTest() {
        final String topic = "topic1";
        final int partition = 12;
        final long offset = 42L;
        final String metadata = "some metadata 123";

        CommitMessage message = new CommitMessage(topic, partition, offset, metadata);

        assertEquals(topic, message.getTopicPartition().topic());
        assertEquals(partition, message.getTopicPartition().partition());
        assertEquals(offset + 1, message.getNextOffset().offset());
        assertEquals(metadata, message.getNextOffset().metadata());
    }

    @Test
    public void beforeTest() {
        final String topic = "topic1";
        final int partition = 1;

        CommitMessage firstMessage = new CommitMessage(topic, partition, 2L, "first message");
        CommitMessage secondMessage = new CommitMessage(topic, partition, 7L, "second message");
        CommitMessage secondMessage2 = new CommitMessage(topic, partition, 7L, "second message");

        assertTrue("first message should be before second message", firstMessage.isBefore(secondMessage));
        assertTrue("second message should be before second message 2", secondMessage.isBefore(secondMessage2));
        assertTrue("second message 2 should be before second message", secondMessage2.isBefore(secondMessage));
        assertFalse("second message 2 shouldn't be before first message", secondMessage2.isBefore(firstMessage));
    }

    @Test(expected = ConnectorException.class)
    public void beforeWithDifferentTopicsTest() {
        final int partition = 0;

        CommitMessage message1 = new CommitMessage("topic1", partition, 1L, "some metadata");

        CommitMessage message2 = new CommitMessage("topic2", partition, 2L, "other metadata");

        message1.isBefore(message2);
    }

    @Test(expected = ConnectorException.class)
    public void beforeWithDifferentPartitionsTest() {
        final String topic = "topic";

        CommitMessage message1 = new CommitMessage(topic, 0, 1L, "some metadata");
        CommitMessage message2 = new CommitMessage(topic, 1, 2L, "other metadata");

        message1.isBefore(message2);
    }

    @Test
    public void equalsAndHashCodeTest() {
        final String topic = "topic";
        final String metadata = "metadata";
        final int partition = 4;
        final long offset = 47L;

        CommitMessage message1 = new CommitMessage(topic, partition, offset, metadata);
        CommitMessage message2 = new CommitMessage(topic, partition, offset, metadata);
        CommitMessage message3 = new CommitMessage("another topic", partition, offset, metadata);
        CommitMessage message4 = new CommitMessage(topic, partition, offset, "other metadata");
        CommitMessage message5 = new CommitMessage(topic, partition, offset + 1, metadata);
        CommitMessage message6 = new CommitMessage(topic, partition + 1, offset, metadata);

        assertEquals(message1, message2);
        assertEquals(message1.hashCode(), message2.hashCode());

        assertNotEquals(message1, message3);
        assertNotEquals(message1.hashCode(), message3.hashCode());

        assertNotEquals(message1, message4);
        assertNotEquals(message1.hashCode(), message4.hashCode());

        assertNotEquals(message1, message5);
        assertNotEquals(message1.hashCode(), message5.hashCode());

        assertNotEquals(message1, message6);
        assertNotEquals(message1.hashCode(), message6.hashCode());

        assertNotNull(message1);
    }
}
