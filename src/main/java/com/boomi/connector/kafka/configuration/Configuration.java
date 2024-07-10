package com.boomi.connector.kafka.configuration;

import com.boomi.connector.kafka.client.common.kerberos.KerberosTicketCache;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import javax.net.ssl.SSLContext;

import java.util.Map;

/**
 * Wrapper for the properties needed to establish a connection with Apache Kafka and configure a Consumer or Producer
 */
public interface Configuration<T extends AbstractConfig> {

    String getClientId();

    String getUsername();

    String getPassword();

    SecurityProtocol getSecurityProtocol();

    SASLMechanism getSaslMechanism();

    String getServiceName();

    ChannelBuilder getChannelBuilder();

    SSLContext getSSLContext();

    Map<String, ?> getValues();

    T getConfig();

    int getMaxRequestSize();

    KerberosTicketCache getTicketCache(String hostname);
}
