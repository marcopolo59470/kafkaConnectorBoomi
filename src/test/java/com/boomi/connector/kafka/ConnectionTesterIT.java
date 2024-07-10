package com.boomi.connector.kafka;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectionTester;
import com.boomi.connector.kafka.configuration.SASLMechanism;
import com.boomi.connector.kafka.util.Constants;

import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

@RunWith(Theories.class)
public class ConnectionTesterIT {

    static {
        TestUtils.disableLogs();
    }

    @DataPoints
    public static Collection<KafkaITContext> contexts() {
        Set<KafkaITContext> contexts = new HashSet<>();

        // plaintext - without sasl
        KafkaITContext plaintextWithNone = new KafkaITContext().setTestConnection();
        plaintextWithNone.addConnectionProperty(Constants.KEY_SERVERS, KafkaITContext.VM_HOST + ":9092");
        plaintextWithNone.addConnectionProperty(Constants.KEY_USERNAME, null);
        plaintextWithNone.addConnectionProperty(Constants.KEY_PASSWORD, null);
        plaintextWithNone.addConnectionProperty(Constants.KEY_SECURITY_PROTOCOL, SecurityProtocol.PLAINTEXT.name);
        plaintextWithNone.addConnectionProperty(Constants.KEY_SASL_MECHANISM, SASLMechanism.NONE.getMechanism());
        contexts.add(plaintextWithNone);

        // plaintext - sasl plain
        KafkaITContext plaintextWithPlain = new KafkaITContext().setTestConnection();
        plaintextWithPlain.addConnectionProperty(Constants.KEY_SERVERS, KafkaITContext.VM_HOST + ":9292");
        plaintextWithPlain.addConnectionProperty(Constants.KEY_USERNAME, "admin");
        plaintextWithPlain.addConnectionProperty(Constants.KEY_PASSWORD, "admin");
        plaintextWithPlain.addConnectionProperty(Constants.KEY_SECURITY_PROTOCOL, SecurityProtocol.SASL_PLAINTEXT.name);
        plaintextWithPlain.addConnectionProperty(Constants.KEY_SASL_MECHANISM, SASLMechanism.PLAIN.getMechanism());
        contexts.add(plaintextWithPlain);

        // plaintext - sasl sha 256
        KafkaITContext plaintextWithSha256 = new KafkaITContext().setTestConnection();
        plaintextWithSha256.addConnectionProperty(Constants.KEY_SERVERS, KafkaITContext.VM_HOST + ":9292");
        plaintextWithSha256.addConnectionProperty(Constants.KEY_USERNAME, "admin");
        plaintextWithSha256.addConnectionProperty(Constants.KEY_PASSWORD, "admin");
        plaintextWithSha256.addConnectionProperty(Constants.KEY_SECURITY_PROTOCOL,
                SecurityProtocol.SASL_PLAINTEXT.name);
        plaintextWithSha256.addConnectionProperty(Constants.KEY_SASL_MECHANISM,
                SASLMechanism.SCRAM_SHA_256.getMechanism());
        contexts.add(plaintextWithSha256);

        // plaintext - sasl sha 512
        KafkaITContext plaintextWithSha512 = new KafkaITContext().setTestConnection();
        plaintextWithSha512.addConnectionProperty(Constants.KEY_SERVERS, KafkaITContext.VM_HOST + ":9292");
        plaintextWithSha512.addConnectionProperty(Constants.KEY_USERNAME, "admin");
        plaintextWithSha512.addConnectionProperty(Constants.KEY_PASSWORD, "admin");
        plaintextWithSha512.addConnectionProperty(Constants.KEY_SECURITY_PROTOCOL,
                SecurityProtocol.SASL_PLAINTEXT.name);
        plaintextWithSha512.addConnectionProperty(Constants.KEY_SASL_MECHANISM,
                SASLMechanism.SCRAM_SHA_512.getMechanism());
        contexts.add(plaintextWithSha512);

        return contexts;
    }

    @Theory
    public void testConnectionTest(BrowseContext context) {
        KafkaConnection<BrowseContext> connection = new KafkaConnection<>(context);
        ConnectionTester connectionTester = new KafkaBrowser(connection);

        connectionTester.testConnection();
    }
}