package com.boomi.connector.kafka.client.common.network;

import com.boomi.connector.kafka.configuration.Configuration;
import com.boomi.connector.kafka.configuration.SASLMechanism;

import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.PlaintextChannelBuilder;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import com.boomi.connector.kafka.client.common.security.BoomiSslFactory;

/**
 * Static Factory to create {@link ChannelBuilder}s
 */
public class BoomiChannelFactory {

    private BoomiChannelFactory() {
    }

    public static ChannelBuilder createChannelBuilder(Configuration<?> configuration) {
        SecurityProtocol protocol = configuration.getSecurityProtocol();
        SASLMechanism mechanism = configuration.getSaslMechanism();

        BoomiSslFactory sslFactory = isSSL(protocol) ? new BoomiSslFactory(configuration.getSSLContext()) : null;

        ChannelBuilder channelBuilder;
        switch (protocol) {
            case SSL:
                channelBuilder = new BoomiSslChannelBuilder(sslFactory);
                break;
            case SASL_SSL:
            case SASL_PLAINTEXT:
                channelBuilder = new BoomiSaslChannelBuilder(sslFactory, mechanism, configuration);
                break;
            case PLAINTEXT:
                ListenerName listener = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT);
                channelBuilder = new PlaintextChannelBuilder(listener);
                channelBuilder.configure(configuration.getValues());
                break;
            default:
                throw new IllegalArgumentException("Unexpected securityProtocol " + protocol);
        }

        return channelBuilder;
    }

    private static boolean isSSL(SecurityProtocol securityProtocol) {
        return (SecurityProtocol.SASL_SSL == securityProtocol) || (SecurityProtocol.SSL == securityProtocol);
    }
}
