package com.boomi.connector.kafka.configuration;

import com.boomi.connector.api.ConnectorContext;
import com.boomi.connector.api.PrivateKeyStore;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.kafka.util.Constants;
import com.boomi.util.NumberUtil;
import com.boomi.util.StringUtil;

import org.apache.kafka.common.security.auth.SecurityProtocol;

import javax.net.ssl.SSLContext;

/**
 * Entity that holds the information needed to connect to Apache Kafka
 */
public class Credentials {

    private final String _username;
    private final String _password;
    private final SecurityProtocol _securityProtocol;
    private final SASLMechanism _saslMechanism;
    private final SSLContext _sslContext;
    private final String _serviceName;
    private final String _oauthTokenUrl;
    private final String _oauthClientId;
    private final String _oauthClientSecret;
    private final String _oauthScope;

    public Credentials(ConnectorContext context, PrivateKeyStore pks) {
        PropertyMap connectionProperties = context.getConnectionProperties();
        // connection properties

        _securityProtocol = NumberUtil.toEnum(SecurityProtocol.class,
                connectionProperties.getProperty(Constants.KEY_SECURITY_PROTOCOL));
        _saslMechanism = SASLMechanism.from(connectionProperties.getProperty(Constants.KEY_SASL_MECHANISM));

        validateProtocolAndMechanism(_securityProtocol, _saslMechanism);

        _username = connectionProperties.getProperty(Constants.KEY_USERNAME);
        _password = connectionProperties.getProperty(Constants.KEY_PASSWORD);
        _oauthTokenUrl = connectionProperties.getProperty(Constants.KEY_OAUTH_TOKEN_URL);
        _oauthClientId = connectionProperties.getProperty(Constants.KEY_OAUTH_CLIENT_ID);
        _oauthClientSecret = connectionProperties.getProperty(Constants.KEY_OAUTH_CLIENT_SECRET);
        _oauthScope = connectionProperties.getProperty(Constants.KEY_OAUTH_SCOPE);

        validateEmptyCredentialsForNonSaslProtocol(_securityProtocol, _username, _password);

        _serviceName = connectionProperties.getProperty(Constants.KEY_SERVICE_PRINCIPAL);

        // SSL context

        _sslContext = (pks != null) ? createSSLContext(pks) : createSSLContext(connectionProperties.getPrivateKeyStoreProperty(Constants.KEY_CERTIFICATE));
        //_sslContext = createSSLContext(connectionProperties);
        //_sslContext = null;
    }

    private SSLContext createSSLContext(PrivateKeyStore pks) {
        SSLContextFactory sslContextFactory = new SSLContextFactory();
        return sslContextFactory.create(pks);
    }

    private static void validateProtocolAndMechanism(SecurityProtocol protocol, SASLMechanism mechanism) {
        boolean isMechanismRequired = isMechanismRequired(protocol);

        boolean isMechanismSet = SASLMechanism.NONE != mechanism;

        if (!isMechanismRequired && isMechanismSet) {
            throw new IllegalArgumentException("SASL Mechanism set, but the selected protocol doesn't make use of it");
        }

        if (isMechanismRequired && !isMechanismSet) {
            throw new IllegalArgumentException("A SASL Mechanism must be specified for the selected protocol");
        }
    }

    private static void validateEmptyCredentialsForNonSaslProtocol(SecurityProtocol protocol, String username,
            String password) {
        if (isMechanismRequired(protocol)) {
            return;
        }

        if (!StringUtil.isEmpty(username)) {
            throw new IllegalArgumentException("A SASL Protocol must be selected to use a username");
        }

        if (!StringUtil.isEmpty(password)) {
            throw new IllegalArgumentException("A SASL Protocol must be selected to use a password");
        }
    }

    private static boolean isMechanismRequired(SecurityProtocol protocol) {
        return (SecurityProtocol.SASL_SSL == protocol) || (SecurityProtocol.SASL_PLAINTEXT == protocol);
    }

    String getUsername() {
        return _username;
    }

    String getPassword() {
        return _password;
    }

    SecurityProtocol getSecurityProtocol() {
        return _securityProtocol;
    }

    SASLMechanism getSaslMechanism() {
        return _saslMechanism;
    }

    SSLContext getSslContext() {
        return _sslContext;
    }

    String getServiceName() {
        return _serviceName;
    }

	public String getOauthTokenUrl() {
		return _oauthTokenUrl;
	}

	public String getOauthClientId() {
		return _oauthClientId;
	}

	public String getOauthClientSecret() {
		return _oauthClientSecret;
	}

	public String getOauthScope() {
		return _oauthScope;
	}
}
