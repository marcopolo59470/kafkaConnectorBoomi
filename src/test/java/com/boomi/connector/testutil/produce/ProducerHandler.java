// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.testutil.produce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ProducerHandler {

    private final ProducerSet _type;
    private final ProducerRecordFactory _recordFactory;
    private final String _topic;

    private ProducerHandler(ProducerSet type, String topic) {
        _type = type;
        _topic = topic;
        _recordFactory = new ProducerRecordFactory(topic);
    }

    public static ProducerHandler fromCluster(String topic) {
        return new ProducerHandler(ProducerSet.CLUSTER, topic);
    }

    public static ProducerHandler fromStandalone(String topic) {
        return new ProducerHandler(ProducerSet.SINGLE, topic);
    }

    public int startProducing(final int amount, final int batchSize, final int producers, final int delay)
            throws InterruptedException {
        final AtomicInteger count = new AtomicInteger();
        final int messagesPerProducer = amount / producers;
        final CountDownLatch signal = new CountDownLatch(producers);

        for (int i = 0; i < producers; i++) {
            final int threadId = i;
            new Thread() {
                public void run() {
                    KafkaProducer<String, String> producer = getProducer(_type);
                    int numberOfBatches = Math.min(messagesPerProducer, messagesPerProducer / batchSize);
                    int batch = Math.min(messagesPerProducer, batchSize);
                    Future<RecordMetadata> lastRecord = null;
                    try {
                        System.out.println(
                                "Thread: " + threadId + " Started producing " + messagesPerProducer + " messages from "
                                        + _topic);
                        for (int x = 0; x < numberOfBatches; x++) {
                            lastRecord = produceMessages(producer, batch, count);
                            producer.flush();

                            TimeUnit.MILLISECONDS.sleep(delay);
                        }
                        if(lastRecord != null){
                            lastRecord.get();
                        }
                    } catch (InterruptedException| ExecutionException v) {
                        System.out.println(v);
                    }


                    System.out.println("Thread: " + threadId + " Complete");
                    signal.countDown();
                }
            }.start();
        }
        signal.await();
        return count.intValue();
    }

    private Future<RecordMetadata> produceMessages(KafkaProducer<String, String> producer, int batchSize,
            AtomicInteger count) {
        Future<RecordMetadata> record = null;
        for (int i = 0; i < batchSize; i++) {
            record = producer.send(_recordFactory.createRecord(count));
        }
        return record;
    }

    private KafkaProducer<String, String> getProducer(ProducerSet type) {
        switch (type) {
            case SINGLE:
                return KafkaProducerFactory.createStandaloneProducer();
            case CLUSTER:
                return KafkaProducerFactory.createClusterProducer();
            default:
                throw new IllegalArgumentException();
        }
    }

    private enum ProducerSet {
        SINGLE, CLUSTER;
    }
}
