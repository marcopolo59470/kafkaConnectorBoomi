package com.boomi.connector.kafka.client.common.network;

import com.boomi.connector.kafka.client.common.security.BoomiSslFactory;
import com.boomi.util.IOUtil;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.network.Authenticator;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.ChannelMetadataRegistry;
import org.apache.kafka.common.network.KafkaChannel;
import org.apache.kafka.common.network.SslTransportLayer;
import org.apache.kafka.common.security.auth.AuthenticationContext;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder;
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde;
import org.apache.kafka.common.security.auth.SslAuthenticationContext;
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

public class BoomiSslChannelBuilder implements ChannelBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(BoomiSslChannelBuilder.class);

    private final BoomiSslFactory _sslFactory;

    BoomiSslChannelBuilder(BoomiSslFactory sslFactory) {
        _sslFactory = sslFactory;
    }

    @Override
    public KafkaChannel buildChannel(String id, SelectionKey key, int maxReceiveSize, MemoryPool memoryPool,
            ChannelMetadataRegistry metadataRegistry) {
        boolean isSuccess = false;
        SslTransportLayer transportLayer = null;
        try {
            memoryPool = memoryPool != null ? memoryPool : MemoryPool.NONE;

            Socket socket = ((SocketChannel) key.channel()).socket();
            String host = socket.getInetAddress().getHostName();
            int port = socket.getPort();

            transportLayer = SslTransportLayer.create(id, key, _sslFactory.createSslEngine(host, port),
                    metadataRegistry);
            Supplier<Authenticator> authenticator = createAuthenticator(transportLayer);
            KafkaChannel kafkaChannel = new KafkaChannel(id, transportLayer, authenticator, maxReceiveSize, memoryPool,
                    metadataRegistry);
            isSuccess = true;
            return kafkaChannel;
        } catch (Exception e) {
            LOG.info("Failed to create channel due to ", e);
            throw new KafkaException(e);
        } finally {
            if (!isSuccess) {
                IOUtil.closeQuietly(transportLayer);
            }
        }
    }

    private static Supplier<Authenticator> createAuthenticator(SslTransportLayer sslTransportLayer) {
        return () -> new SslAuthenticator(sslTransportLayer);
    }

    @Override
    public void configure(Map<String, ?> configs) {
        //no-op
    }

    @Override
    public void close() {
        //no-op
    }

    /**
     * Note that client SSL authentication is handled in {@link SslTransportLayer}. This class is only used to transform
     * the derived principal using a {@link KafkaPrincipalBuilder} configured by the user.
     */
    private static class SslAuthenticator implements Authenticator {

        private final SslTransportLayer _transportLayer;
        private final KafkaPrincipalBuilder _principalBuilder;

        private SslAuthenticator(SslTransportLayer transportLayer) {
            _transportLayer = transportLayer;
            _principalBuilder = new DefaultKafkaPrincipalBuilder(null, null);
        }

        /**
         * No-Op for plaintext authenticator
         */
        @Override
        public void authenticate() {
            //no-op
        }

        /**
         * Constructs Principal using configured _principalBuilder.
         *
         * @return the built principal
         */
        @Override
        public KafkaPrincipal principal() {
            InetAddress clientAddress = _transportLayer.socketChannel().socket().getInetAddress();
            AuthenticationContext context = new SslAuthenticationContext(_transportLayer.sslSession(),
                    clientAddress, null);
            return _principalBuilder.build(context);
        }

        @Override
        public Optional<KafkaPrincipalSerde> principalSerde() {
            return Optional.empty();
        }

        @Override
        public void close() {
            if (_principalBuilder instanceof AutoCloseable) {
                Utils.closeQuietly((AutoCloseable) _principalBuilder, "principal builder");
            }
        }

        /**
         * SslAuthenticator doesn't implement any additional authentication mechanism.
         *
         * @return true
         */
        @Override
        public boolean complete() {
            return true;
        }
    }
}
