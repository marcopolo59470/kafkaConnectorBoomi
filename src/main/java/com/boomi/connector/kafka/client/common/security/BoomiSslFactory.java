package com.boomi.connector.kafka.client.common.security;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

/**
 * A factory to create {@link SSLEngine}s from the {@link SSLContext} provided on construction.
 */
public class BoomiSslFactory {

    private final SSLContext _sslContext;

    public BoomiSslFactory(SSLContext sslContext) {
        _sslContext = sslContext;
    }

    public SSLEngine createSslEngine(String peerHost, int peerPort) {
        SSLEngine sslEngine = _sslContext.createSSLEngine(peerHost, peerPort);
        sslEngine.setUseClientMode(true);
        return sslEngine;
    }
}
