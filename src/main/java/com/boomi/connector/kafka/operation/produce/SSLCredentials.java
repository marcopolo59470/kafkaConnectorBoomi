package com.boomi.connector.kafka.operation.produce;

public class SSLCredentials {

    private final String credentialSource;
    private final String accessKey;
    private final String accessCertificate;
    private final String caCertificate;
    private final String bootstrapServer;
    private final String schemaUrl;

    public SSLCredentials(String credentialSource, String accessKey, String accessCertificate, String caCertificate, String bootstrapServer, String schemaUrl) {
        this.credentialSource = credentialSource;
        this.accessKey = accessKey;
        this.accessCertificate = accessCertificate;
        this.caCertificate = caCertificate;
        this.bootstrapServer = bootstrapServer;
        this.schemaUrl = schemaUrl;
    }

    public String getCredentialSource() {
        return credentialSource;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getAccessCertificate() {
        return accessCertificate;
    }

    public String getCACertificate() {
        return caCertificate;
    }

    public String getBootstrapServer() {
        return bootstrapServer;
    }

    public String getSchemaUrl() {
        return schemaUrl;
    }
}

