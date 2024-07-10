// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.testutil.consume;

import com.boomi.connector.kafka.configuration.SASLMechanism;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.Map;

public class KafkaConsumerBuilder {

    private SASLMechanism _mechanism = SASLMechanism.PLAIN;
    private SecurityProtocol _protocol = SecurityProtocol.PLAINTEXT;
    private String _clientId;
    private long _timeout = 60_000;
    private StringBuilder _host = new StringBuilder();
    private String _groupId;
    private String _offsetReset = "earliest";

    public KafkaConsumerBuilder(String clientId, String groupId) {
        _clientId = clientId;
        _groupId = groupId;
    }

    public KafkaConsumerBuilder withHost(String host, String port) {
        if(_host.length() > 0){
            _host.append(",");
        }
        _host.append(host).append(":").append(port);
        return this;
    }

    public KafkaConsumerBuilder withMechanism(SASLMechanism mechanism) {
        _mechanism = mechanism;
        return this;
    }

    public KafkaConsumerBuilder withProtocol(SecurityProtocol protocol) {
        _protocol = protocol;
        return this;
    }

    public KafkaConsumerBuilder withProtocol(long timeout) {
        _timeout = timeout;
        return this;
    }

    public KafkaConsumerBuilder withOffsetReset(String offsetReset) {
        _offsetReset = offsetReset;
        return this;
    }

    public KafkaConsumer<String, String> build() {
        Map<String, Object> configs = new HashMap<>();
        configs.put("bootstrap.servers", _host.toString());
        configs.put("request._timeout.ms", _timeout);
        configs.put("security.protocol", _protocol.name);
        configs.put("sasl.mechanism", _mechanism.getMechanism());
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, _groupId);
        configs.put(ConsumerConfig.CLIENT_ID_CONFIG, _clientId);
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, _offsetReset);
        return new KafkaConsumer<>(configs);
    }
}
