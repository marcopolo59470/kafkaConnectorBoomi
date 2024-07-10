// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.kafka.client.common.kerberos;

import com.boomi.util.EqualsBuilder;
import com.boomi.util.StringUtil;

import org.apache.kerby.kerberos.kerb.type.base.PrincipalName;

import java.io.Serializable;
import java.util.Objects;

/**
 * This class wraps all the values needed to authenticate against a GSSAPI-Kerberos Distribution Center and uniquely
 * identify the credentials stored in ConnectorCache,
 */
public class KerberosTicketKey implements Serializable {

    private static final long serialVersionUID = 20200415L;

    private static final String REALM_TEMPLATE = "@%s";
    private static final String SERVICE_PRINCIPAL_TEMPLATE = "%s/%s@%s";

    private final String _realm;
    private final String _clientPrincipal;
    private final String _password;
    private final String _servicePrincipal;

    /**
     * Constructs a new instance using the values provided. If the value provided for the serviceName argument is not a
     * proper PrincipalName, this class will try to build a valid one using the provided hostname and the Realm
     * contained in the Client Principal
     *
     * @param hostName
     *         The FQDN of the server in which the Kafka Broker is running. This value is fetched from the connection
     *         socket while the SASL Channel is being constructed.
     * @param serviceName
     *         the Service Principal Name configured in the connection.
     * @param clientPrincipal
     *         the Client Principal configured in the connection
     * @param password
     *         the password String configured in the connection.
     */
    public KerberosTicketKey(String hostName, String serviceName, String clientPrincipal, String password) {
        _clientPrincipal = clientPrincipal;
        _password = password;

        // get the realm from the principal, an IllegalArgumentException is thrown if the realm is missing
        _realm = PrincipalName.extractRealm(clientPrincipal);
        _servicePrincipal = buildServicePrincipal(serviceName, hostName, _realm);
    }

    private static String buildServicePrincipal(String serviceName, String host, String realm) {
        String formattedRealm = String.format(REALM_TEMPLATE, realm);
        if (StringUtil.endsWith(serviceName, formattedRealm)) {
            return serviceName;
        }
        return String.format(SERVICE_PRINCIPAL_TEMPLATE, serviceName, host, realm);
    }

    /**
     * Access method to get the Realm part extracted from the Client Principal
     *
     * @return the Client Principal's realm as a String
     */
    String getRealm() {
        return _realm;
    }

    /**
     * Access method to get the Client Principal
     *
     * @return the Client Principal
     */
    String getClientPrincipal() {
        return _clientPrincipal;
    }

    /**
     * Access method to get the password field
     *
     * @return the password field asa String
     */
    String getPassword() {
        return _password;
    }

    /**
     * Access method to get the fully formed Service Principal
     *
     * @return the Service Principal as a String
     */
    String getServicePrincipal() {
        return _servicePrincipal;
    }

    /**
     * If the password is set, it will be used over the keytab file
     *
     * @return true if the password is set, false otherwise
     */
    boolean shouldUsePassword() {
        return StringUtil.isNotEmpty(_password);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KerberosTicketKey)) {
            return false;
        }

        KerberosTicketKey that = (KerberosTicketKey) o;
        return new EqualsBuilder()
                .append(_clientPrincipal, that._clientPrincipal)
                .append(_servicePrincipal, that._servicePrincipal)
                .append(_password, that._password).isEquals();
    }

    @Override
    public int hashCode() {
        return Objects.hash(_clientPrincipal, _servicePrincipal, _password);
    }
}
