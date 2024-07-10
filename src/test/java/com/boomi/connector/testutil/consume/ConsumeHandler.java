// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.testutil.consume;

import com.boomi.util.IOUtil;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.Closeable;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class ConsumeHandler implements Closeable {

    private final KafkaConsumer<String, String> _consumer;
    private final String _topic;
    private int _count;

    private ConsumeHandler(KafkaConsumer<String, String> consumer, String topic) {
        _topic = topic;
        _consumer = consumer;
    }

    public static ConsumeHandler fromCluster(String topic, String clientId, String groupId) {
        return new ConsumeHandler(KafkaConsumerFactory.createClusterConsumer(clientId, groupId), topic);
    }

    public static ConsumeHandler fromStandalone(String topic, String clientId, String groupId) {
        return new ConsumeHandler(KafkaConsumerFactory.createStandaloneConsumer(clientId, groupId), topic);
    }

    public void forceRebalance() {
        _consumer.subscribe(Collections.singleton(_topic));
        _consumer.poll(0L);
        _consumer.unsubscribe();
        this.close();
    }

    public void startConsume(long runningTime, final long delay, final int maxNumberMessages) {
        final long timeout = System.currentTimeMillis() + runningTime;
        try {
            _consumer.subscribe(Collections.singleton(_topic));
            do {
                ConsumerRecords<String, String> records = _consumer.poll(0L);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record);
                    _count++;
                }
                TimeUnit.MILLISECONDS.sleep(delay);
            } while ((System.currentTimeMillis() < timeout) && _count < maxNumberMessages);
            stopConsume();
        } catch (InterruptedException v) {
            System.out.println(v);
        }
    }

    public void stopConsume() {
        _consumer.unsubscribe();
    }

    public int getCount() {
        return _count;
    }

    @Override
    public void close() {
        IOUtil.closeQuietly(_consumer);
    }
}
