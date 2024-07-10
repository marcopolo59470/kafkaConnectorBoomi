
package com.boomi.connector.kafka.configuration;

import com.boomi.connector.kafka.KafkaITContext;
import com.boomi.connector.kafka.util.Constants;
import com.boomi.util.StringUtil;

import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class CredentialsTest {

    @DataPoints
    public static final SecurityProtocol[] SECURITY_PROTOCOLS = SecurityProtocol.values();
    @DataPoints
    public static final SASLMechanism[] SASL_MECHANISMS = SASLMechanism.values();
    @DataPoints
    public static final String[] USERNAME_PASSWORD_WITH_NULL = new String[] {
            StringUtil.EMPTY_STRING, "validString" };

    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";

    private static boolean isSASL(SecurityProtocol protocol) {
        return (protocol == SecurityProtocol.SASL_SSL) || (protocol == SecurityProtocol.SASL_PLAINTEXT);
    }

    @Theory
    public void shouldSuccessSaslWithUsernamePasswordTest(SecurityProtocol protocol, SASLMechanism mechanism) {
        // ignore non SASL protocols
        Assume.assumeTrue(isSASL(protocol));

        // ignore NONE mechanism
        Assume.assumeTrue(mechanism != SASLMechanism.NONE);

        // prepare context
        KafkaITContext context = new KafkaITContext();
        context.addConnectionProperty(Constants.KEY_SECURITY_PROTOCOL, protocol.name);
        context.addConnectionProperty(Constants.KEY_SASL_MECHANISM, mechanism.getMechanism());
        context.addConnectionProperty(Constants.KEY_USERNAME, USERNAME);
        context.addConnectionProperty(Constants.KEY_PASSWORD, PASSWORD);

        // execute expecting no errors
        new Credentials(context);
    }

    @Theory
    public void shouldSuccessNonSaslWithoutUsernamePasswordTest(SecurityProtocol protocol, SASLMechanism mechanism) {
        // ignore non SASL protocols
        Assume.assumeTrue(!isSASL(protocol));

        // use only NONE mechanism
        Assume.assumeTrue(mechanism == SASLMechanism.NONE);

        // prepare context
        KafkaITContext context = new KafkaITContext();
        context.addConnectionProperty(Constants.KEY_SECURITY_PROTOCOL, protocol.name);
        context.addConnectionProperty(Constants.KEY_SASL_MECHANISM, mechanism.getMechanism());

        // execute expecting no errors
        new Credentials(context);
    }

    @Theory
    public void shouldFailForNonSaslWithUsernameAndPasswordTest(SecurityProtocol protocol, String username,
            String password) {
        // ignore SASL Protocols
        Assume.assumeTrue(!isSASL(protocol));

        // at least one string not empty
        Assume.assumeTrue(StringUtil.isNotEmpty(username) || StringUtil.isNotEmpty(password));

        // prepare context
        KafkaITContext context = new KafkaITContext();
        context.addConnectionProperty(Constants.KEY_SECURITY_PROTOCOL, protocol.name);
        context.addConnectionProperty(Constants.KEY_SASL_MECHANISM, SASLMechanism.NONE.getMechanism());
        context.addConnectionProperty(Constants.KEY_USERNAME, username);
        context.addConnectionProperty(Constants.KEY_PASSWORD, password);

        // execute expecting errors
        try {
            new Credentials(context);
        } catch (IllegalArgumentException e) {
            return;
        }
        Assert.fail();
    }

    @Theory(nullsAccepted = false)
    public void shouldFailForNonSaslWithNonNoneMechanism(SecurityProtocol protocol, SASLMechanism mechanism) {
        // ignore SASL Protocols
        Assume.assumeTrue(!isSASL(protocol));

        // try all mechanisms except for NONE
        Assume.assumeTrue(mechanism != SASLMechanism.NONE);

        // prepare context
        KafkaITContext context = new KafkaITContext();
        context.addConnectionProperty(Constants.KEY_SECURITY_PROTOCOL, protocol.name);
        context.addConnectionProperty(Constants.KEY_SASL_MECHANISM, mechanism.getMechanism());

        // execute expecting errors
        try {
            new Credentials(context);
        } catch (IllegalArgumentException e) {
            return;
        }
        Assert.fail();
    }

    @Test
    public void shouldFailForSaslWithNoneMechanism() {
        // prepare context
        KafkaITContext context = new KafkaITContext();
        context.addConnectionProperty(Constants.KEY_SECURITY_PROTOCOL, SecurityProtocol.SASL_SSL.name);
        context.addConnectionProperty(Constants.KEY_SASL_MECHANISM, SASLMechanism.NONE.getMechanism());
        context.addConnectionProperty(Constants.KEY_USERNAME, USERNAME);
        context.addConnectionProperty(Constants.KEY_PASSWORD, PASSWORD);

        // execute expecting errors
        try {
            new Credentials(context);
        } catch (IllegalArgumentException e) {
            return;
        }
        Assert.fail();
    }
}
