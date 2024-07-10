# ADR #2 - Support multiple Service Principals

JIRA Ticket: https://boomii.atlassian.net/browse/BOOMI-42245

The Kerberos authentication in the Kafka Connector required the introduction of a new field called Service Principal 
Name (SPN).

This value is used to indicate the complete SPN that identifies the service in Kerberos and consists on three main parts:

                 {service_name}/{hostname}@{realm}

With this field in addition to the already existing fields for “Client Principal” and the List of Brokers to connect to, 
kafka is able to identify both the user and the destination server.

For this approach to work, all the brokers configured in the connection needs to be authenticated against Kerberos to 
use the same SPN, however, it is possible to use an unique SPN for each broker and this generates a handshake failure 
on the rotation.


### Possible Enhancement 

The configuration of the Kafka Clients has the default option of providing a list of brokers (bootstrap servers) but 
only a single value to identify the service in Kerberos

It should be possible to enhance the connector to allow the possibility of providing either the complete SPN or just 
the Service Name (first part of the SPN) and allow the connector to infer the rest by:

- Fetching the actual hostname from the connection socket.
- Taking the KRB Realm from the Client principal.

This mechanism relies on a well configured environment in which:

- All the hosts are accessible by their FQDN.
- The Service Principals are created by specifying the external hostname that will be returned by the server in which 
they run.
- There is an accessible KDC in those hosts.

In that way the connector could determine if an SPN was configured in the connection to make a straightforward usage 
(current approach) or to build a full SPN when only the Service Name was provided. Making it compatible with the current
 implementation and less error prone to any of the constraints mentioned above by allowing users to manually configure 
 the SPN and avoid the automatic process in those circumstances in which the network does not complies with the 
 constraints specified in this document.


### Code Updated
- A new class name KerberosTicketKey was created to hold the information needed to retrieve and uniquely identify a 
Service Granting Ticket on the ConnectorCache. This class has the responsibility for determine the type of value entered
in the Connection Field called Service Principal Name and build a proper one if only the Service Name was configured.
- KafkaConfiguration.getTicketCache was updated to receive an instance of KerberosTicketKey to identify the object
that will be stored in ConnectorCache
- BoomiGssClientFactory receives a full configuration when it's created and the destination hostname when the 
createSaslClient method is invoked. With this information it creates a new instance of a KerberosTicketKey used to 
request a KerberosTicketCache to the configuration
- The class KerberosTicketCache was updated to handle a synchronized retrieval of the SgtTicket from Kerberos and the 
previous KerberosTicketsFactory was removed. This change was introduced in order to avoid the necessity to provide a full
Configuration and a ConnectorContext when a new instance of BoomiGssclient is created.
- BoomiGssClient will receive an implementation of SgtTicketProvider (KerberosTicketCached) on its constructor and will
 directly provide it to the GssContext.
- The label and help text of the connection field with id service_principal were updated to reflect this change.

