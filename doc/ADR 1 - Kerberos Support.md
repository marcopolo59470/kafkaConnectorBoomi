# ADR #1 - Implement Support for Kerberos Authentication

JIRA Ticket: https://boomii.atlassian.net/browse/BOOMI-40329

Provided support for Kerberos in the connector is a challenging task because Java (and the Kafka Client) handle the 
authentication through JAAS and the Java GSS-API. This has proven to be problematic on Cloud Atoms because the
SecurityManager will prevent the execution of privileged actions and the access to classes stored within many of 
the sun.security package.
 
In addition to those security concerns, the only mechanism provided by the JGSS API to load the krb configuration 
and the keytab are system-wide and depends on permissions to load them from the filesystem. Making it unsuitable for 
shared environments and Multi tenancy.

With this in mind, the problem can be split in two main subjects:

1. Load a custom krb5.conf & keytab credentials in the connector.
2. Orchestrate the Kerberos Authentication without the need to register any provider nor using JAAS.


#### Discovery

There were a few options discussed for #1, going from supporting only Password-based auth, serializing the keytab
file into a connection field or providing it through a Custom Library jar.  

The first approach investigated to solve #2 was to extends JGSS classes in order to inject custom behavior but it was
quickly dropped because most of the classes in that API are final and referencing them will still cause issues with 
Cloud Atoms. Once that it was clear that using the mechanisms provided by the JDK was not possible, it was decided to
 look for a different solution.
 
After some investigation, an open source library: [Apache Kerby] (https://directory.apache.org/kerby) 
was found and being open source, distributed under the Apache 2 License, making it suitable to be patched to apply
any change required to be integrated into the connector.

Some aspects worth of mention in Kerby are:
* Built upon JDK 8.
* Allows a programmatic injection of the krb5.conf file, allowing multi-tenancy.
* Does not requires JAAS and works correctly in Cloud Atoms.
* It is possible to execute each step of the authentication by separate. (TGT, SGT, Auth Context).

 
#### Decision

Upon further analysis and discussions it was decided to:

1. Design the connector in such a way that the connector will load the KDC setup (krb5.conf) and the Credentials from a
Keytab file that will be provided in a Custom Library deployed to the container.

2. Proceed to include Apache Kerby as a connector dependency and patch it to generate an API that allows the injection
of the resources mentioned in #1. 

#### Status

Changes in the Connector:
* Apache Kerby's kerb-client & kerb-gssapi were included as connector dependencies.
* A KerberosTicketsFactory was implemented to load the KDC options and the Credentials, execute the requests to retrieve
the TGT & SGT tickets from Kerberos and finally store the last one in Connector Cache.
* The SGT is generated with the same expiration date as the original TGT and it was therefore decided to only cache the
SGT that will be needed to authenticate against the service (Kafka). There could be a small benefit in caching the TGT 
to reuse it if the connector has to authenticate against a different Kafka broker running with the same Kerberos Server
credentials but it was dismissed for this phase as the benefit is marginal considering the maintainability impact.
* A custom SaslClient called BoomiGssClient was implemented and injected to the communication channel through the 
existing BoomiSaslChannelBuilder.

Changes in the Apache Kerby:
* The Kerberos client was updated to allow the injection of the resources as InputStreams instead of a file while the 
original constructors and methods were preserved and are still valid.
* A new simplified facade for the GssContext was implemented to provide a simplified API that also allows the injection
of an already retrieved SGT Ticket.
* Kafka requires the inclusion of a Checksum calculation in the Authenticator signature of Service Requests, Kerby lacked
this functionality and it had to be included as an optional option in the newly created DirectGSSContext.