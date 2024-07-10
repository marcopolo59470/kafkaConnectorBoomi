// Copyright (c) 2019 Boomi, Inc.
package com.boomi.connector.kafka.client.common.security;

import com.boomi.connector.kafka.configuration.Configuration;
import com.boomi.util.StringUtil;

import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.scram.ScramExtensionsCallback;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;

import java.util.List;
import java.util.Map;

/**
 * Custom callback handler for Sasl Clients. It injects the username and password to the callback when required.
 */
public class BoomiSaslCallbackHandler implements AuthenticateCallbackHandler {

    private final String _username;
    private final char[] _password;

    public BoomiSaslCallbackHandler(Configuration<?> configuration) {
        _username = configuration.getUsername();
        _password = StringUtil.defaultIfBlank(configuration.getPassword(), StringUtil.EMPTY_STRING).toCharArray();
    }

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof ScramExtensionsCallback) {
                // Scram extensions are not used in our impl.
                continue;
            }

            if (callback instanceof NameCallback) {
                ((NameCallback) callback).setName(_username);
            } else if (callback instanceof PasswordCallback) {
                ((PasswordCallback) callback).setPassword(_password);
            } else if (callback instanceof RealmCallback) {
                RealmCallback rc = (RealmCallback) callback;
                rc.setText(rc.getDefaultText());
            } else if (callback instanceof AuthorizeCallback) {
                AuthorizeCallback ac = (AuthorizeCallback) callback;
                String authId = ac.getAuthenticationID();
                String authzId = ac.getAuthorizationID();
                ac.setAuthorized(authId.equals(authzId));
                if (ac.isAuthorized()) {
                    ac.setAuthorizedID(authzId);
                }
            } else {
                throw new UnsupportedCallbackException(callback, "Unrecognized SASL ClientCallback");
            }
        }
    }

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        //no-op
    }

    @Override
    public void close() {
        //no-op
    }
}
