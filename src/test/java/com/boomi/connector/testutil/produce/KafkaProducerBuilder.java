// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.testutil.produce;

import com.boomi.connector.kafka.configuration.SASLMechanism;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

public class KafkaProducerBuilder {

    private SASLMechanism _mechanism = SASLMechanism.PLAIN;
    private SecurityProtocol _protocol = SecurityProtocol.PLAINTEXT;
    private boolean _acknowledge = true;
    private long _timeout = 60_000;
    private StringBuilder _host = new StringBuilder();

    public KafkaProducerBuilder() {
    }

    public KafkaProducerBuilder withHost(String host, String port) {
        if (_host.length() > 0) {
            _host.append(", ");
        }
        _host.append(host).append(":").append(port);
        return this;
    }

    public KafkaProducerBuilder withMechanism(SASLMechanism mechanism) {
        _mechanism = mechanism;
        return this;
    }

    public KafkaProducerBuilder withProtocol(SecurityProtocol protocol) {
        _protocol = protocol;
        return this;
    }

    public KafkaProducerBuilder withAcknowledges(boolean acknowledges) {
        _acknowledge = acknowledges;
        return this;
    }

    public KafkaProducerBuilder withProtocol(long timeout) {
        _timeout = timeout;
        return this;
    }

    public KafkaProducer<String, String> build() {
        Map<String, Object> configs = new HashMap<>();
        configs.put("bootstrap.servers", _host.toString());
        configs.put("request._timeout.ms", _timeout);
        configs.put("security.protocol", _protocol.name);
        configs.put("sasl.mechanism", _mechanism.getMechanism());
        configs.put(ProducerConfig.ACKS_CONFIG, (_acknowledge ? "all" : "0"));
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer<>(configs);
    }
}
