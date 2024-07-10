// Copyright (c) 2019 Boomi, Inc.
package com.boomi.connector.kafka.client.common.kerberos;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.kafka.configuration.Configuration;
import com.boomi.connector.util.ConnectorCache;

import org.apache.kerby.kerberos.kerb.gss.impl.ticket.SgtTicketProvider;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslClientFactory;

import java.util.Map;

/**
 * Custom implementation of {@link SaslClientFactory} for creating a {@link BoomiGssClient}. The Kerberos SGT Ticket
 * needed for instantiating the Client is retrieved from the {@link ConnectorCache}. This factory was created to
 * preserve its caller behaviour, which instantiates a {@link SaslClientFactory} and calls createSaslClient.
 */
public class BoomiGssClientFactory implements SaslClientFactory {

    private static final String[] GSSAPI = new String[] { "GSSAPI" };

    private final Configuration<?> _configuration;

    public BoomiGssClientFactory(Configuration<?> configuration) {
        _configuration = configuration;
    }

    @Override
    public SaslClient createSaslClient(String[] mechanisms, String authorizationId, String protocol, String serverName,
            Map<String, ?> props, CallbackHandler cbh) {
        try {
            SgtTicketProvider provider = _configuration.getTicketCache(serverName);
            return new BoomiGssClient(props, provider);
        } catch (Exception e) {
            throw new ConnectorException(e);
        }
    }

    @Override
    public String[] getMechanismNames(Map<String, ?> props) {
        return GSSAPI;
    }
}
