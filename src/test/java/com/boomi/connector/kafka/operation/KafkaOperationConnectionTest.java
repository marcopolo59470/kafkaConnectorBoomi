// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.kafka.operation;

import com.boomi.connector.kafka.KafkaITContext;
import com.boomi.connector.kafka.operation.KafkaOperationConnection;

import org.junit.Assert;
import org.junit.Test;

public class KafkaOperationConnectionTest {

    @Test
    public void getTopicTest() {
        final String expectedTopic = "expectedTopic";

        KafkaITContext context = KafkaITContext.produceVM();
        context.setObjectTypeId(expectedTopic);
        KafkaOperationConnection connection = new KafkaOperationConnection(context);

        String topic = connection.getObjectTypeId();

        Assert.assertEquals(expectedTopic, topic);
    }
}
