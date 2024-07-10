
package com.boomi.connector.kafka.configuration;

import com.boomi.util.StringUtil;

import org.junit.Assert;
import org.junit.Test;

public class SASLMechanismTest {

    @Test
    public void fromStringTest() {
        for (SASLMechanism mechanism : SASLMechanism.values()) {
            Assert.assertEquals(mechanism, SASLMechanism.from(mechanism.getMechanism()));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void fromInvalidMechanismTest() {
        SASLMechanism.from("invalid");
    }

    @Test(expected = IllegalArgumentException.class)
    public void fromEmptyMechanismTest() {
        SASLMechanism.from(StringUtil.EMPTY_STRING);
    }
}
