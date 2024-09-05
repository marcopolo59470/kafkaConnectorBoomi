package com.boomi.connector.kafka.client.common.network;

import com.boomi.connector.kafka.client.common.kerberos.BoomiGssClientFactory;
import com.boomi.connector.kafka.client.common.security.BoomiSaslCallbackHandler;
import com.boomi.connector.kafka.client.common.security.BoomiSslFactory;
import com.boomi.connector.kafka.configuration.Configuration;
import com.boomi.connector.kafka.configuration.SASLMechanism;
import com.boomi.util.IOUtil;
import com.sun.security.sasl.ClientFactoryImpl;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.network.Authenticator;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.ChannelMetadataRegistry;
import org.apache.kafka.common.network.KafkaChannel;
import org.apache.kafka.common.network.PlaintextTransportLayer;
import org.apache.kafka.common.network.SslTransportLayer;
import org.apache.kafka.common.network.TransportLayer;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.authenticator.SaslClientAuthenticator;
import org.apache.kafka.common.security.scram.internals.ScramSaslClient;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslClientFactory;
import javax.security.sasl.SaslException;

import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class BoomiSaslChannelBuilder implements ChannelBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(BoomiSaslChannelBuilder.class);

    private static final String SERVICE_PRINCIPAL = "kafka";
    private static final boolean HANDSHAKE_REQUEST_ENABLE = true;
    //Max Receive Size for Authentication
    private static final int MAX_RECEIVE_SIZE = 524288;

    private final String _clientSaslMechanism;
    private final BoomiSslFactory _sslFactory;
    private Configuration<?> _configuration;

    BoomiSaslChannelBuilder(BoomiSslFactory sslFactory, SASLMechanism mechanism, Configuration<?> configuration) {
        if ((SASLMechanism.NONE == mechanism) || (mechanism == null)) {
            throw new IllegalArgumentException("SASL mechanism cannot be NONE or null for a SASL protocol");
        }
        _clientSaslMechanism = mechanism.getMechanism();
        _configuration = configuration;
        _sslFactory = sslFactory;
    }

    @Override
    public KafkaChannel buildChannel(String id, SelectionKey key, int maxReceiveSize, MemoryPool memoryPool,
            ChannelMetadataRegistry registry) {
        boolean isSuccess = false;
        TransportLayer transportLayer = null;
        Socket socket = null;
        Supplier<Authenticator> authenticatorSupplier;

        try {
            memoryPool = memoryPool != null ? memoryPool : MemoryPool.NONE;

            socket = ((SocketChannel) key.channel()).socket();
            String hostName = socket.getInetAddress().getHostName();

            transportLayer = (_sslFactory == null) ? new PlaintextTransportLayer(key) : SslTransportLayer.create(id,
                    key, _sslFactory.createSslEngine(hostName, socket.getPort()), registry);

            authenticatorSupplier  = createAuthenticator(transportLayer, id, hostName);

            KafkaChannel kafkaChannel = new KafkaChannel(id, transportLayer, authenticatorSupplier, maxReceiveSize,
                    memoryPool, registry);
            isSuccess = true;
            return kafkaChannel;
        } catch (Exception e) {
            LOG.info("Failed to create channel due to ", e);
            throw new KafkaException(e);
        } finally {
            if (!isSuccess) {
                IOUtil.closeQuietly(socket, transportLayer);
            }
        }
    }

    private Supplier<Authenticator> createAuthenticator(TransportLayer transportLayer, String id, String hostName) {
        Map<String, String> configs = new HashMap<>();
        configs.put(CommonClientConfigs.CLIENT_ID_CONFIG, _configuration.getClientId());
        configs.put(CommonClientConfigs.PRINCIPAL_NAME, _configuration.getUsername());

        AuthenticateCallbackHandler callbackHandler = new BoomiSaslCallbackHandler(_configuration);
        return () -> new SaslClientAuthenticator(configs, callbackHandler, id, SERVICE_PRINCIPAL, hostName,
                _clientSaslMechanism, MAX_RECEIVE_SIZE, HANDSHAKE_REQUEST_ENABLE, transportLayer, new LogContext(),
                getSaslClient(hostName));
    }

    private SaslClient getSaslClient(String host) {
        try {
            String[] mechs = { _clientSaslMechanism };
            SaslClientFactory factory = getClientFactory(_configuration.getSaslMechanism());
            return factory.createSaslClient(mechs, null, SERVICE_PRINCIPAL, host, _configuration.getValues(),
                    new BoomiSaslCallbackHandler(_configuration));
        } catch (SaslException e) {
            final String errorMessage = "Failed to create SaslClient with mechanism " + _clientSaslMechanism;
            LOG.warn(errorMessage, e);
            throw new SaslAuthenticationException(errorMessage, e.getCause());
        }
    }

    private SaslClientFactory getClientFactory(SASLMechanism mechanism) throws SaslException {
        switch (mechanism) {
            case PLAIN:
                return new ClientFactoryImpl();
            case SCRAM_SHA_256:
            case SCRAM_SHA_512:
                return new ScramSaslClient.ScramSaslClientFactory();
            case GSSAPI:
                return new BoomiGssClientFactory(_configuration);
            default:
                throw new SaslException("The mechanism is not valid: " + mechanism);
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        //no-op
    }

    @Override
    public void close() {
        //Nothing to do
    }
}
