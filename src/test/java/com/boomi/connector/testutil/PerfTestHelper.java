//Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.testutil;

import com.boomi.connector.testutil.consume.ConsumeHandler;
import com.boomi.connector.testutil.produce.ProducerHandler;

import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class PerfTestHelper {

    private static final String TOPIC = "2-partitions";
    private static final String CLIENT_ID = "gaston-1-partition";
    private static final String CONSUMER_GROUP = "gaston-1-partition";
    private static final int MESSAGES = 100000;
    private static final int BATCH_SIZE = 100;
    private static final int PRODUCERS = 30;
    private static final int DELAY = 0;

    @Test
    public void produceMessage() throws InterruptedException {
        ProducerHandler handler = ProducerHandler.fromStandalone(TOPIC);
        long start = System.currentTimeMillis();
        int count = handler.startProducing(MESSAGES, BATCH_SIZE, PRODUCERS, DELAY);
        System.out.println("Total messages produced: " + count);
        System.out.println("Elapsed Time: " + (System.currentTimeMillis() - start));
    }

    //@Test
    public void forceRebalance() {
        ConsumeHandler handler = ConsumeHandler.fromCluster(TOPIC, CLIENT_ID, CONSUMER_GROUP);
        handler.forceRebalance();
    }

    //@Test
    public void consumeTest() {
        ConsumeHandler handler = ConsumeHandler.fromCluster(TOPIC, CLIENT_ID, CONSUMER_GROUP);
        handler.startConsume(9_000L, 0L, 0);
        System.out.println("message count: " + handler.getCount());
    }
}