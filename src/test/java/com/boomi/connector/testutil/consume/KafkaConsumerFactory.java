// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.testutil.consume;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaConsumerFactory {

    public static KafkaConsumer<String, String> createClusterConsumer(String clientId, String hostId) {
        KafkaConsumerBuilder builder = new KafkaConsumerBuilder(clientId, hostId)
                .withHost("54.209.70.187", "9090")
                .withHost("54.209.70.187", "9091")
                .withHost("54.209.70.187", "9092")
                .withHost("54.209.70.187", "9093");

        return builder.build();
    }

    public static KafkaConsumer<String, String> createStandaloneConsumer(String clientId, String hostId) {
        return new KafkaConsumerBuilder(clientId, hostId).withHost("18.205.74.232","9092").build();
    }

}
