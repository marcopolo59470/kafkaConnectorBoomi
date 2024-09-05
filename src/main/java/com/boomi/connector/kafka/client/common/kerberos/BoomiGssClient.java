package com.boomi.connector.kafka.client.common.kerberos;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.kafka.client.common.network.BoomiSaslChannelBuilder;
import com.boomi.util.LogUtil;
import com.sun.security.sasl.util.AbstractSaslImpl;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.security.authenticator.SaslClientAuthenticator;
import org.apache.kerby.kerberos.kerb.gss.impl.DirectGssContext;
import org.apache.kerby.kerberos.kerb.gss.impl.ticket.SgtTicketProvider;
import org.ietf.jgss.MessageProp;

import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Custom client which implements {@link SaslClient} and leverages on {@link AbstractSaslImpl}. This client will be
 * injected by the {@link BoomiSaslChannelBuilder} into the {@link SaslClientAuthenticator} in order to authenticate
 * against Kerberos.
 */
class BoomiGssClient extends AbstractSaslImpl implements SaslClient {

    private static final Logger LOG = LogUtil.getLogger(BoomiGssClient.class);

    private static final byte INTEGRITY_ONLY_PROTECTION = (byte) 2;
    private static final byte PRIVACY_PROTECTION = (byte) 4;
    // unrelated to SASL QOP mask
    private static final int JGSS_QOP = 0;
    private static final byte[] EMPTY = new byte[0];
    private static final int GSS_TOKEN_LENGTH = 4;
    private static final int BUFFER_SIZE = 3;

    private final SgtTicketProvider _sgtTicketProvider;

    private DirectGssContext _context;
    private boolean _finalHandshake;

    /**
     * Constructs a Custom {@link SaslClient} capable of handling Kerberos Authentication.
     *
     * The creation of the {@link DirectGssContext} is delayed until method evaluateChallenge is invoked to allow
     * exceptions to be directly captured by the {@link SaslClientAuthenticator}. Any exception thrown in this
     * constructor would be logged and discarded by {@link org.apache.kafka.clients.NetworkClient#ready(Node, long)}.
     *
     * @param props
     *         properties to initialize the client
     * @param sgtTicketProvider
     *         an {@link SgtTicketProvider} instance with valid credentials to be authenticated against the Service
     *         Principal.
     * @throws SaslException
     *         if an error happened initializing the client
     */
    BoomiGssClient(Map<String, ?> props, SgtTicketProvider sgtTicketProvider) throws SaslException {
        // the class name is passed to the parent class for logging purposes
        super(props, BoomiGssClient.class.getName());
        _sgtTicketProvider = sgtTicketProvider;
    }

    /**
     * Determines whether this mechanism has an optional initial response. If true, caller should call
     * evaluateChallenge() with an empty array to get the initial response. In this implementation evaluateChallenge
     * will always be called.
     *
     * @return true if this mechanism has an initial response.
     */
    @Override
    public boolean hasInitialResponse() {
        return true;
    }

    /**
     * Evaluates the challenge data and generates a response. If a challenge is received from the server during the
     * authentication process, this method is called to prepare an appropriate next response to submit to the server.
     *
     * @param challengeData
     *         The non-null challenge sent from the server. The challenge array may have zero length.
     * @return The possibly null reponse to send to the server. It is null if the challenge accompanied a "SUCCESS"
     * status and the challenge only contains data for the client to update its state and no response needs to be sent
     * to the server. The response is a zero-length byte array if the client is to send a response with no data.
     * @throws SaslException
     *         If an error occurred while processing the challenge or generating a response.
     */
    public byte[] evaluateChallenge(byte[] challengeData) throws SaslException {
        if (completed) {
            throw new IllegalStateException("GSSAPI authentication already complete");
        }

        if (_context == null) {
           _context = prepareContext();
        }

        if (_finalHandshake) {
            return doFinalHandshake(challengeData);
        } else {

            try {
                byte[] gssOutToken = _context.initSecContext(challengeData, challengeData.length);
                if (_context.isEstablished()) {
                    _finalHandshake = true;
                    if (gssOutToken == null) {
                        // RFC 2222 7.2.1:  Client responds with no data
                        return EMPTY;
                    }
                }

                return gssOutToken;
            } catch (Exception e) {
                String message = "GSS initiate failed";
                LOG.log(Level.WARNING, message, e);
                throw new SaslAuthenticationException(message, e);
            }
        }
    }

    private DirectGssContext prepareContext() {
        try{
            return new DirectGssContext(_sgtTicketProvider);
        }catch (ConnectorException e){
            Throwable t = (e.getCause() != null)? e.getCause(): e;
            LOG.log(Level.WARNING, t.getMessage(), t);
            throw new SaslAuthenticationException(t.getMessage());
        }
    }

    /**
     * Determines whether the authentication exchange has completed. This method may be called at any time, but
     * typically, it will not be called until the caller has received indication from the server (in a protocol-specific
     * manner) that the exchange has completed.
     *
     * @return true if the authentication exchange has completed; false otherwise.
     */
    @Override
    public boolean isComplete() {
        return completed;
    }

    /**
     * Returns the IANA-registered mechanism name of this SASL client. (e.g. "CRAM-MD5", "GSSAPI").
     *
     * @return A non-null string representing the IANA-registered mechanism name.
     */
    @Override
    public String getMechanismName() {
        return "GSSAPI";
    }

    /**
     * Unwraps a byte array received from the server. This method can be called only after the authentication exchange
     * has completed (i.e., when isComplete() returns true) and only if the authentication exchange has negotiated
     * integrity and/or privacy as the quality of protection; otherwise, an IllegalStateException is thrown. incoming is
     * the contents of the SASL buffer as defined in RFC 2222 without the leading four octet field that represents the
     * length. offset and len specify the portion of incoming to use.
     *
     * @param incoming
     *         A non-null byte array containing the encoded bytes from the server.
     * @param start
     *         The starting position at incoming of the bytes to use.
     * @param len
     *         The number of bytes from incoming to use.
     * @return A non-null byte array containing the decoded bytes.
     * @throws SaslException
     *         if incoming cannot be successfully unwrapped.
     * @throws IllegalStateException
     *         if the authentication exchange has not completed, or if the negotiated quality of protection has neither
     *         integrity nor privacy.
     */
    @Override
    public byte[] unwrap(byte[] incoming, int start, int len) throws SaslException {
        validateCompleteAndIntegrity();

        try {
            MessageProp msgProp = new MessageProp(JGSS_QOP, privacy);
            return _context.unwrap(incoming, start, len, msgProp);
        } catch (Exception e) {
            throw new SaslException("Problems unwrapping SASL buffer", e);
        }
    }

    /**
     * Wraps a byte array to be sent to the server. This method can be called only after the authentication exchange has
     * completed (i.e., when isComplete() returns true) and only if the authentication exchange has negotiated integrity
     * and/or privacy as the quality of protection; otherwise, an IllegalStateException is thrown. The result of this
     * method will make up the contents of the SASL buffer as defined in RFC 2222 without the leading four octet field
     * that represents the length. offset and len specify the portion of outgoing to use.
     *
     * @param outgoing
     *         A non-null byte array containing the bytes to encode.
     * @param start
     *         The starting position at outgoing of the bytes to use.
     * @param len
     *         The number of bytes from outgoing to use.
     * @return A non-null byte array containing the encoded bytes.
     * @throws SaslException
     *         if outgoing cannot be successfully wrapped.
     * @throws IllegalStateException
     *         if the authentication exchange has not completed, or if the negotiated quality of protection has neither
     *         integrity nor privacy.
     */
    public byte[] wrap(byte[] outgoing, int start, int len) throws SaslException {
        validateCompleteAndIntegrity();

        // Generate GSS token
        try {
            MessageProp msgProp = new MessageProp(JGSS_QOP, privacy);
            return _context.wrap(outgoing, start, len, msgProp);
        } catch (Exception e) {
            throw new SaslException("Problem performing GSS wrap", e);
        }
    }

    private void validateCompleteAndIntegrity() {
        if (_context == null) {
            throw new IllegalStateException("GSSAPI context not available");
        }

        if (!completed) {
            throw new IllegalStateException("GSSAPI authentication not completed");
        }

        // integrity will be true if either privacy or integrity negotiated
        if (!integrity) {
            throw new IllegalStateException("No security layer negotiated");
        }
    }

    /**
     * Retrieves the negotiated property. This method can be called only after the authentication exchange has completed
     * (i.e., when isComplete() returns true); otherwise, an IllegalStateException is thrown.
     *
     * @param propName
     *         The non-null property name.
     * @return The value of the negotiated property. If null, the property was not negotiated or is not applicable to
     * this mechanism.
     * @throws IllegalStateException
     *         if this authentication exchange has not completed
     */
    @Override
    public Object getNegotiatedProperty(String propName) {
        return null;
    }

    @Override
    public void dispose() {
        // do nothing
    }

    private byte[] doFinalHandshake(byte[] challengeData) throws SaslException {
        try {
            if (challengeData.length == 0) {
                // Received S0, should return []
                return EMPTY;
            }

            // Received S1 (security layer, server max recv size)

            byte[] gssOutToken = _context.unwrap(challengeData, 0, challengeData.length, new MessageProp(0, false));

            // First octet is a bit-mask specifying the protections
            // supported by the server

            // Client selects preferred protection
            // qop is ordered list of qop values
            byte selectedQop = findPreferredMask(gssOutToken[0], qop);
            if (selectedQop == 0) {
                throw new SaslException("No common protection layer between client and server");
            }

            if ((selectedQop & PRIVACY_PROTECTION) != 0) {
                privacy = true;
                integrity = true;
            } else if ((selectedQop & INTEGRITY_ONLY_PROTECTION) != 0) {
                integrity = true;
            }

            // 2nd-4th octets specifies maximum buffer size expected by
            // server (in network byte order)
            int srvMaxBufSize = networkByteOrderToInt(gssOutToken, 1, BUFFER_SIZE);

            // Determine the max send buffer size based on what the
            // server is able to receive and our specified max
            sendMaxBufSize = (sendMaxBufSize == 0) ? srvMaxBufSize : Math.min(sendMaxBufSize, srvMaxBufSize);

            // Update _context to limit size of returned buffer
            rawSendSize = _context.getWrapSizeLimit(JGSS_QOP, privacy, sendMaxBufSize);

            // Construct negotiated security layers and client's max
            // receive buffer size and authzID
            byte[] gssInToken = new byte[GSS_TOKEN_LENGTH];
            gssInToken[0] = selectedQop;

            intToNetworkByteOrder(recvMaxBufSize, gssInToken, 1, BUFFER_SIZE);

            gssOutToken = _context.wrap(gssInToken, 0, gssInToken.length,
                    new MessageProp(0 /* qop */, false /* privacy */));

            // server authenticated
            completed = true;

            return gssOutToken;
        } catch (Exception e) {
            throw new SaslException("Final handshake failed", e);
        }
    }
}
