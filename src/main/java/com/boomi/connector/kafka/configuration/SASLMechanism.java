
package com.boomi.connector.kafka.configuration;

import com.boomi.util.StringUtil;

/**
 * Representation of the different SASL Mechanisms supported by the connector.
 */
public enum SASLMechanism {
    NONE("NONE"), PLAIN("PLAIN"), SCRAM_SHA_256("SCRAM-SHA-256"), SCRAM_SHA_512("SCRAM-SHA-512"), GSSAPI("GSSAPI");

    private final String _mechanism;

    SASLMechanism(String mechanism) {
        _mechanism = mechanism;
    }

    /**
     * Return the {@link SASLMechanism} from the given mechanism name
     *
     * @param mechanism
     *         the mechanism name
     * @return the {@link SASLMechanism}
     * @throws IllegalArgumentException
     *         if the given name is blank or invalid
     */
    public static SASLMechanism from(String mechanism) {
        if (StringUtil.isBlank(mechanism)) {
            throw new IllegalArgumentException("SASL Mechanism cannot be blank");
        }

        for (SASLMechanism saslMechanism : values()) {
            if (saslMechanism.getMechanism().equalsIgnoreCase(mechanism)) {
                return saslMechanism;
            }
        }

        throw new IllegalArgumentException("Unknown SASL Mechanism: " + mechanism);
    }

    /**
     * @return the name of the SASL Mechanism
     */
    public String getMechanism() {
        return _mechanism;
    }
}