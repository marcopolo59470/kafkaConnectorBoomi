//Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.kafka.operation.polling.execution;

import com.boomi.connector.api.listen.Listener;
import com.boomi.connector.api.listen.PayloadBatch;
import com.boomi.connector.kafka.KafkaITContext;
import com.boomi.connector.kafka.client.consumer.ConsumeMessage;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Map;

public class ProcessExecutionTest {

    private static final String TOPIC_NAME = "topic";

    private static ConsumeMessage createMessage(int partition, long offset) {
        return new ConsumeMessage(new TopicPartition(TOPIC_NAME, partition), offset);
    }

    private static TopicPartition topic(int partition) {
        return new TopicPartition(TOPIC_NAME, partition);
    }

    @Test
    public void addTest() {
        Listener listener = Mockito.mock(Listener.class);
        PayloadBatch payloadBatch = Mockito.mock(PayloadBatch.class);
        Mockito.when(listener.getBatch()).thenReturn(payloadBatch);
        Mockito.when(payloadBatch.getCount()).thenReturn(8);

        ProcessExecution processExecution = new ProcessExecution(payloadBatch, new KafkaITContext(),
                false);

        processExecution.add(createMessage(0, 1L));
        processExecution.add(createMessage(0, 2L));
        processExecution.add(createMessage(0, 3L));
        processExecution.add(createMessage(0, 4L));
        processExecution.add(createMessage(0, 5L));

        processExecution.add(createMessage(1, 0L));
        processExecution.add(createMessage(1, 1L));
        processExecution.add(createMessage(1, 2L));

        processExecution.submit();

        Map<TopicPartition, OffsetAndMetadata> startPosition = processExecution.startOffsets();
        Map<TopicPartition, OffsetAndMetadata> lastPosition = processExecution.endOffsets();

        Assert.assertEquals(1L, startPosition.get(topic(0)).offset());
        Assert.assertEquals(0L, startPosition.get(topic(1)).offset());

        Assert.assertEquals(6L, lastPosition.get(topic(0)).offset());
        Assert.assertEquals(3L, lastPosition.get(topic(1)).offset());
    }
}
