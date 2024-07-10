package com.boomi.connector.kafka.client.common.kerberos;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.util.ConnectorCache;
import com.boomi.util.ClassUtil;
import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;

import org.apache.kerby.kerberos.kerb.KrbException;
import org.apache.kerby.kerberos.kerb.client.KrbClient;
import org.apache.kerby.kerberos.kerb.client.KrbConfig;
import org.apache.kerby.kerberos.kerb.gss.impl.ticket.SgtTicketProvider;
import org.apache.kerby.kerberos.kerb.keytab.Keytab;
import org.apache.kerby.kerberos.kerb.type.KerberosTime;
import org.apache.kerby.kerberos.kerb.type.ticket.SgtTicket;
import org.apache.kerby.kerberos.kerb.type.ticket.TgtTicket;

import java.io.IOException;
import java.io.InputStream;
import java.text.MessageFormat;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation of {@link ConnectorCache} for storing a Kerberos Service Granting Ticket.
 *
 * This class provides a custom implementation of {@link ConnectorCache#isValid()} that checks the end time of the
 * ticket invalidating the instance if any is expired.
 */
public class KerberosTicketCache extends ConnectorCache<KerberosTicketKey> implements SgtTicketProvider {
    private static final Logger LOG = LogUtil.getLogger(KerberosTicketCache.class);

    private static final String KRB_5_CONF = "krb5.conf";
    private static final String FILE_NOT_FOUND = "{0} file not found";
    private static final String ERROR_LOADING_KRB_CONFIG = "Failed to load krb config";
    private static final String ERROR_LOADING_KEYTAB = "Failed to load keytab file";
    private static final String KEYTAB_PATTERN = "{0}.keytab";
    private static final String ERROR_CREATING_TICKETS = "error getting the Granting Tickets";
    private static final String GOT_TGT_TICKET = "Got TGT Ticket with expiration date {0}";
    private static final String GOT_SGT_TICKET = "Got SGT Ticket with expiration date {0}";

    //Sets a padding skew of 5 minutes to reduce the risk of returning an invalid ticket. The time was chosen
    // accordingly to the example provided in RFC-1510 and the default value in MIT Kerberos.
    private static final long PADDING_SKEW = 300000L;

    private final KrbConfig _krbConfig;

    private SgtTicket _sgtTicket;

    /**
     * Constructs a new instance using the values provided in the key.
     * It relies on two files provided in a custom library:
     * 'krb5.conf' a configuration file with information about the key distribution center.
     * '{REALM}.keytab' a keytab valid for the client principal specified in the connection.
     *
     */
    public KerberosTicketCache(KerberosTicketKey key) {
        super(key);
        _krbConfig = createConfig();
    }

    /**
     *
     *
     * @return the Kerberos Service Granting Ticket
     */
    @Override
    public synchronized SgtTicket getSgtTicket() {
        if(_sgtTicket == null){
            _sgtTicket = refreshTicket(getKey());
        }
        return _sgtTicket;
    }

    /**
     * Check if the Ticket held on the instance is expired
     *
     * @return true if the SGT Tickets is not expired, false otherwise
     */
    @Override
    public boolean isValid() {
        //having a null _sgtTicket means that this object has not been yet used to retrieve the actual ticket from
        // kerberos and it is thus valid
        if(_sgtTicket == null){
            return true;
        }
        KerberosTime now = new KerberosTime();
        return _sgtTicket.getEncKdcRepPart().getEndTime().greaterThanWithSkew(now, PADDING_SKEW);
    }

    /**
     * Returns a new instance of a {@link KerberosTicketCache} containing the Kerberos Granting Tickets for the given
     * key
     *
     * @param key
     *         the key composed of the client principal and service principal by which this ConnectorCache will be
     *         cached in the connector cache
     * @return the KerberosTicketCache instance
     */
    private SgtTicket refreshTicket(KerberosTicketKey key) {

        try {
            KrbClient client = new KrbClient(_krbConfig);
            client.init();

            TgtTicket tgtTicket;
            if (key.shouldUsePassword()) {
                tgtTicket = client.requestTgt(key.getClientPrincipal(), key.getPassword());
            } else {
                // get the keytab resource from the custom library deployed by the user.
                // the file name is composed of the realm name and the extension '.keytab'
                tgtTicket = client.requestTgt(key.getClientPrincipal(), getKeytab(key.getRealm()));
            }

            if (LOG.isLoggable(Level.FINE)) {
                LOG.log(Level.FINE, MessageFormat.format(GOT_TGT_TICKET, tgtTicket.getEncKdcRepPart().getEndTime()));
            }

            SgtTicket sgtTicket = client.requestSgt(tgtTicket, key.getServicePrincipal());

            if (LOG.isLoggable(Level.FINE)) {
                LOG.log(Level.FINE, MessageFormat.format(GOT_SGT_TICKET, sgtTicket.getEncKdcRepPart().getEndTime()));
            }

            return sgtTicket;
        } catch (KrbException e) {
            LOG.log(Level.WARNING, ERROR_CREATING_TICKETS, e);
            throw new ConnectorException(e);
        }
    }

    /**
     * Create an instance of KrbConfig from the file 'krb5.conf' located in the custom library uploaded by the user.
     *
     * @return the config
     */
    private static KrbConfig createConfig() {
        InputStream krb5 = null;

        try {
            krb5 = loadResource(KRB_5_CONF);
            KrbConfig krbConfig = new KrbConfig();
            krbConfig.addStreamKrb5Config(krb5);
            return krbConfig;
        } catch (IOException e) {
            LOG.log(Level.WARNING, ERROR_LOADING_KRB_CONFIG, e);
            throw new ConnectorException(ERROR_LOADING_KRB_CONFIG, e);
        } finally {
            IOUtil.closeQuietly(krb5);
        }
    }

    /**
     * Load a resource file into an InputStream validating that the file exists
     *
     * @return the InputStream representing the krb5.conf file
     */
    private static InputStream loadResource(String resourceName) {
        InputStream resourceAsStream = ClassUtil.getResourceAsStream(resourceName);
        if (resourceAsStream == null) {
            throw new ConnectorException(MessageFormat.format(FILE_NOT_FOUND, resourceName));
        }
        return resourceAsStream;
    }

    /**
     * Builds the keytab file name appending <code>.keytab</code> to the given String and loads it into a
     * {@link Keytab}
     *
     * @param realm
     *         the prefix of the keytab filename
     * @return the {@link Keytab} instance
     */
    private static Keytab getKeytab(String realm) {
        String filename = MessageFormat.format(KEYTAB_PATTERN, realm);
        InputStream keytabFile = null;
        try {
            keytabFile = loadResource(filename);
            return Keytab.loadKeytab(keytabFile);
        } catch (IOException e) {
            LOG.log(Level.WARNING, ERROR_LOADING_KEYTAB, e);
            throw new ConnectorException(ERROR_LOADING_KEYTAB, e);
        } finally {
            IOUtil.closeQuietly(keytabFile);
        }
    }

}
