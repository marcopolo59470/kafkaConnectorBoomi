package com.boomi.connector.kafka.util;

import com.boomi.connector.api.ConnectorException;
import com.boomi.util.StringUtil;

import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class PartitionUtilTest {

    private static final String TOPIC = "topicTest";
    private static final String PARTITION_IDS = "1,2,3";

    @Test(expected = ConnectorException.class)
    public void givenEmptyStringThenExceptionIsThrown() {
        PartitionUtil.createTopicPartition(StringUtil.EMPTY_STRING, TOPIC);
    }

    @Test(expected = ConnectorException.class)
    public void givenNullValueThenExceptionIsThrown() {
        PartitionUtil.createTopicPartition( TOPIC, null);
    }

    @Test
    public void given3PartitionIdsThenTopicPartitionsListIsReturned() {
        List<TopicPartition> topicPartitions = PartitionUtil.createTopicPartition(TOPIC, PARTITION_IDS);

        Assert.assertNotNull(topicPartitions);
        Assert.assertEquals(3, topicPartitions.size());
    }
}