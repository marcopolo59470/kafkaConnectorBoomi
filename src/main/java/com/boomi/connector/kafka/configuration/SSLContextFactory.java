
package com.boomi.connector.kafka.configuration;

import com.boomi.component.CertificateFactory;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.PrivateKeyStore;
import com.boomi.execution.CurrentExecutionContext;
import com.boomi.execution.ExecutionContext;
import com.boomi.util.security.SecurityUtil;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import java.security.GeneralSecurityException;
import java.security.KeyStore;

/**
 * Factory class to create a TLS 1.2 {@link SSLContext} with the custom certificates deployed in the platform.
 */
public class SSLContextFactory {

    /**
     * This method is a combination of
     * 1. {@link SecurityUtil#getTLS12Context}, and
     * 2. {@link com.boomi.execution.ExecutionUtil#createSSLContext}
     * as it needs to return a TLS V1.2 context (1) with the custom certificates deployed from the platform (2).
     *
     * @return the TLS v1.2 SSLContext with custom certificates from the platform
     */
    public SSLContext create(PrivateKeyStore privateKeystore) {
        try {
            ExecutionContext executionContext = CurrentExecutionContext.get();
            KeyStore trustStore =
                    (executionContext == null) ? SecurityUtil.loadCaCerts() : CertificateFactory.getInstance(
                            executionContext.getAccountConfig()).getTrustStoreIfAvailable();

            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trustStore);

            KeyManager[] keyManagers = null;
            if (privateKeystore != null) {
                String kmfAlgorithm = KeyManagerFactory.getDefaultAlgorithm();
                KeyManagerFactory kmf = KeyManagerFactory.getInstance(kmfAlgorithm);
                KeyStore keyStore = privateKeystore.getKeyStore();
                kmf.init(keyStore, privateKeystore.getPassword().toCharArray());
                keyManagers = kmf.getKeyManagers();
            }

            SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
            sslContext.init(keyManagers, tmf.getTrustManagers(), null);
            return sslContext;
        } catch (GeneralSecurityException e) {
            throw new ConnectorException("the SSL context could not be created", e);
        }
    }
}
