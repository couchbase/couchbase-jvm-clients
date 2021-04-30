
Instructions for Exercising Java Client OSGI Bundle
---------------------------------------------------

Start by building everything:

$ mkdir sdk
# cd sdk
$ git clone git@github.com:aaronjwhiteside/spring-data-couchbase.git
$ cd core-io-deps
$ mvn install
$ cd ..
$ mvn install
$ 

In the following instructions for karaf,  specify the same version numbers that were just built.
Pay special attention to versions with the suffix "-SNAPSHOT" (Or ".SNAPSHOT").

The bundle includes an Activator which saves and gets a document 
to "travel-sample" on Couchbase Server running on localhost

Create bucket "travel-sample" in the Couchbase console either manually or from Settings -> Sample Buckets

Download and install karaf

Add the following to org.osgi.framework.bootdelegation in <karaf>/etc/config.properties
     javax.crypto, \
     javax.crypto.*, \
     javax.security.sasl, \
     javax.security.auth.*, \
     javax.security.cert, \
     javax.security.cert.*, \

Add the feature repository to etc/org.apache.karaf.features.repos.cfg

osgi-feature=mvn:com.couchbase.client/osgi-feature/1.2.0.SNAPSHOT/xml/features

Add the feature to etc/org.apache.karaf.features.cfg featureRepositories:

  mvn:com.couchbase.client/osgi-feature/1.2.0.SNAPSHOT/xml/features

In this directory there are sample files:

	java-examples/config.properties
	java-examples/org.apache.karaf.features.cfg
	java-examples/org.apache.karaf.features.repos.cfg

Clear the karaf cache.  Do this whenever you have new jars.
It will also uninstall installed features and unbundles.

$ rm -r <karaf-dir>/data/cache

$ karaf
karaf@root()> stack-traces-print
karaf@root()> feature:install osgi-feature
# if karaf exits at this point with "java.io.IOException: Stream Closed",
# simply restart karaf and continue
$ karaf
karaf@root()> stack-traces-print
karaf@root()> install mvn:com.couchbase.client/java-examples/1.2.0-SNAPSHOT
Bundle ID: 302
start 302
Hello world.
HOME: /Users/username
log4j.configuration file:///Users/username/log4j.properties
Cluster.connect...
GetResult{content={"name":"MyName"}, flags=0x2000000, cas=0x167aa6e785c60000, expiry=Optional.empty}
karaf@root()> stop 302
Goodbye world.
karaf@root()> uninstall 302
karaf@root()> 
^D
$


To test the Bundle without leveraging the "feature"
===================================================

$ karaf
karaf@root()> 
stack-traces-print
install mvn:io.projectreactor/reactor-core/3.4.4
install mvn:org.reactivestreams/reactive-streams/1.0.3
install mvn:org.slf4j/slf4j-log4j12/1.7.30 # runs without this, but needed for logging to work
install mvn:org.slf4j/slf4j-api/1.7.30 # runs without this, but needed for logging to work
install mvn:log4j/log4j/1.2.17 # need this one to run # will get NoClassDefFoundError: org/apache/log4j/LogManager from reactor.
install mvn:com.couchbase.client/core-io/2.1.4-SNAPSHOT
#
# if you exit karaf here (^D), then restart karaf and then install the java-client
# the logging from $HOME/log4j.properties will take effect
# Otherwise you will not have the log4j logging
#
$ karaf
stack-traces-print
install mvn:com.couchbase.client/java-client/3.1.4-SNAPSHOT
install mvn:com.couchbase.client/java-examples/1.1.4-SNAPSHOT
Bundle ID: 302
karaf@root()> start 302
Hello world.
HOME: /Users/username
log4j.configuration file:///Users/username/log4j.properties
Cluster.connect...
GetResult{content={"name":"MyName"}, flags=0x2000000, cas=0x167aa6e785c60000, expiry=Optional.empty}
karaf@root()> stop 302
Goodbye world.
karaf@root()> uninstall 302
karaf@root()> 
^D
$

$HOME/log4j.properties - this is the log4j configuration for core-io-deps (netty etc). 
(Using an xml file requires extra karaf/osgi configuration to access additional xml parsing classes)

# Root logger option
log4j.rootLogger=INFO, stdout

# Direct log messages to stdout
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.Target=System.out
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n

#log4j.logger.com.couchbase=DEBUG
