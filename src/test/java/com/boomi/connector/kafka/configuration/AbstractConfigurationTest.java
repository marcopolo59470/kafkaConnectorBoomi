
package com.boomi.connector.kafka.configuration;

import com.boomi.connector.kafka.KafkaITContext;
import com.boomi.connector.kafka.util.Constants;
import com.boomi.util.ByteUnit;

import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.Assert;

public abstract class AbstractConfigurationTest {

    protected static final int DEFAULT_MAX_FETCH_SIZE = (int) ByteUnit.byteSize(10, ByteUnit.MB.name());
    protected static final int DEFAULT_POLL_SIZE = 1;

    private static final String EXPECTED_USERNAME = "username";
    private static final String EXPECTED_PASSWORD = "username";
    private static final SecurityProtocol EXPECTED_PROTOCOL = SecurityProtocol.SASL_SSL;
    private static final SASLMechanism EXPECTED_MECHANISM = SASLMechanism.SCRAM_SHA_512;


    protected static KafkaITContext getContextDouble() {
        KafkaITContext context = new KafkaITContext();

        context.addConnectionProperty(Constants.KEY_SECURITY_PROTOCOL, SecurityProtocol.SASL_SSL.name);
        context.addConnectionProperty(Constants.KEY_SASL_MECHANISM, SASLMechanism.SCRAM_SHA_512.getMechanism());
        context.addConnectionProperty(Constants.KEY_SERVERS, "https://somebroker.com");

        context.addConnectionProperty(Constants.KEY_USERNAME, EXPECTED_USERNAME);
        context.addConnectionProperty(Constants.KEY_PASSWORD, EXPECTED_PASSWORD);
        return context;
    }

    protected static void assertCredentials(Configuration<?> configuration) {
        Assert.assertEquals(EXPECTED_PROTOCOL, configuration.getSecurityProtocol());
        Assert.assertEquals(EXPECTED_MECHANISM, configuration.getSaslMechanism());
        Assert.assertEquals(EXPECTED_USERNAME, configuration.getUsername());
        Assert.assertEquals(EXPECTED_PASSWORD, configuration.getPassword());
    }
}
