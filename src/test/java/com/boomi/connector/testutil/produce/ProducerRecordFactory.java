// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.testutil.produce;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ProducerRecordFactory {

    private final static String PAYLOAD = "Payload #%s Aliquam lacinia nisl a commodo interdum. Curabitur vel justo "
            + "ligula. Aenean posuere venenatis turpis, eget cursus risus ultrices hendrerit. Vestibulum elementum, "
            + "metus non aliquet bibendum, tortor odio pharetra sem, eget pulvinar tellus diam vitae diam.";

    private final String _topic;
    private int _index = 0;

    public ProducerRecordFactory(String topic) {
        _topic = topic;
    }

    public List<ProducerRecord<String, String>> createRecords(AtomicInteger index, int amount) {
        List<ProducerRecord<String, String>> records = new ArrayList<>(amount);
        for (int i = 0; i < amount; i++) {
            records.add(createRecord(index));
        }
        return records;
    }

    public ProducerRecord<String, String> createRecord(AtomicInteger index) {
        return new ProducerRecord<>(_topic, String.valueOf(index.getAndIncrement()),String.format(PAYLOAD, _index));
    }
}
