package com.boomi.connector.kafka.util;

import com.boomi.connector.api.ConnectorException;
import com.boomi.util.StringUtil;

import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Auxiliary class responsible the reading the given topic and partitions, and create a list of {@liTopicPartition}
 */
public final class PartitionUtil {

    private PartitionUtil() {
    }

    /**
     * Create a list of {@link TopicPartition} with the given topic and partitions.
     *
     * @param topic
     *         The topic name.
     * @param partitions
     *         The partition ids
     * @return a new list of {@link TopicPartition}.
     */
    public static List<TopicPartition> createTopicPartition(String topic, String partitions) {
        if (StringUtil.isBlank(partitions)) {
            throw new ConnectorException("Partition(s) must be assign");
        }
        String[] ids = StringUtil.fastSplit(",", partitions);
        try {
            return Arrays.stream(ids).map(StringUtil::trim).map(Integer::parseInt).map(
                    partition -> new TopicPartition(topic, partition)).collect(Collectors.toList());
        } catch (NumberFormatException e) {
            throw new ConnectorException("Partition id must be a number", e);
        }
    }
}