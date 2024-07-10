// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.testutil.produce;

import org.apache.kafka.clients.producer.KafkaProducer;

public class KafkaProducerFactory {

    public static KafkaProducer<String, String> createClusterProducer() {
        KafkaProducerBuilder builder = new KafkaProducerBuilder()
                .withHost("54.209.70.187", "9091")
                .withHost("54.209.70.187", "9092")
                .withHost("54.209.70.187", "9093");

        return builder.build();
    }

    public static KafkaProducer<String, String> createStandaloneProducer() {
        return  new KafkaProducerBuilder().withHost("localhost","9092").build();
    }
}
