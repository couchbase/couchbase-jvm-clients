# Couchbase JVM Clients

This repository contains the next generation of Couchbase SDKs ("3.0") as well as a brand-new `core-io` library.

It is currently under development and not ready for prime-time.

**THERE BE DRAGONS**

## Overview

This repository contains the following projects:

 - `core-io`: the core library for all language bindings
 - `java-client`: the java language binding
 - `scala-client`: the scala language binding
 - `kotlin-client`: the kotlin language binding
 
Regular Couchbase users will note that both the scala and the kotlin binding are new! Yes they are, and we are trying to develop them as first-class citizens similar to the java client going forward.
 
## Usage

Since we are not publishing builds yet, you need to do a local checkout and build.

```
git clone https://github.com/couchbaselabs/couchbase-jvm-clients.git
cd couchbase-jvm-clients
mvn -Dscala.compat.version=2.12 clean install
mvn -Dscala.compat.version=2.11 clean install
```

(The two `mvn` runs are to cross-compile the Scala SDK for Scala 2.11 and 2.12.)

### Building and Testing

You can test like this:

```
$ mvn --fail-at-end clean test
```

And building:

```
mvn -Dscala.compat.version=2.12 clean install
mvn -Dscala.compat.version=2.11 clean install
```

Use `-Dmaven.test.skip=true` to skip testing.

(You can always go into one of the sub-directories like `core-io` to only build or test an individual project.)

### Testing Infos

To cover all tests, the suite needs to be run against the following topologies:

 - 1 node, no replica
 - 2 nodes, 1 replica
 - 2 nodes, 2 replicas
 
### Snapshot packaging
After building:

```
tar -czf java-client-alpha.0.tar.gz java-client/target/java-client-alpha.0.jar java-client/target/java-client-alpha.0-javadoc.jar java-client/target/java-client-alpha.0-sources.jar java-client/pom.xml
tar -czf scala-client-alpha.0.tar.gz scala-client/target/scala-client_2.12-alpha.0.jar scala-client/target/scala-client_2.12-alpha.0-sources.jar scala-client/target/scala-client_2.12-alpha.0-javadoc.jar scala-client/pom.xml
```