// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.kafka.client.common.kerberos;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

//The Apache Kerby library supports only Jdk 8 onwards so we don't want to run these tests when the connector is built
// in bamboo
@Ignore
public class KerberosTicketKeyTest {

    private static final String PRINCIPAL_NAME_TEMPLATE = "%s/%s@%s";
    private static final String HOST = "host.com";
    private static final String REALM = "KAFKA_REALM";
    private static final String SERVICE_NAME = "kafka";
    private static final String PASSWORD = "secret";
    private static final String CLIENT_PRINCIPAL = String.format(PRINCIPAL_NAME_TEMPLATE, "user", HOST, REALM);
    private static final String SERVICE_PRINCIPAL = String.format(PRINCIPAL_NAME_TEMPLATE, SERVICE_NAME, HOST, REALM);

    @Test
    public void shouldCorrectlyBuildAKerberosTicketKey() {

        KerberosTicketKey key = new KerberosTicketKey(HOST, SERVICE_PRINCIPAL, CLIENT_PRINCIPAL, PASSWORD);

        assertEquals(CLIENT_PRINCIPAL, key.getClientPrincipal());
        assertEquals(PASSWORD, key.getPassword());
        assertEquals(REALM, key.getRealm());
        assertEquals(SERVICE_PRINCIPAL, key.getServicePrincipal());
        assertTrue(key.shouldUsePassword());

        KerberosTicketKey passwordlessKey = new KerberosTicketKey(HOST, SERVICE_NAME, CLIENT_PRINCIPAL, null);
        assertFalse(passwordlessKey.shouldUsePassword());
    }

    @Test
    public void servicePrincipalTest() {

        KerberosTicketKey key = new KerberosTicketKey(HOST, SERVICE_PRINCIPAL, CLIENT_PRINCIPAL, PASSWORD);
        assertEquals(SERVICE_PRINCIPAL, key.getServicePrincipal());

        key = new KerberosTicketKey(HOST, SERVICE_NAME, CLIENT_PRINCIPAL, PASSWORD);
        assertEquals(SERVICE_PRINCIPAL, key.getServicePrincipal());

        String clientPrincipal = String.format(PRINCIPAL_NAME_TEMPLATE, "user", HOST, SERVICE_NAME);
        key = new KerberosTicketKey(HOST, SERVICE_NAME, clientPrincipal, "secret");
        assertEquals("kafka/host.com@kafka", key.getServicePrincipal());
    }
}
