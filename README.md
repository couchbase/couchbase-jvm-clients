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
$ git clone https://github.com/couchbaselabs/couchbase-jvm-clients.git
$ cd couchbase-jvm-clients
$ mvn -Dscala.compat.version=2.11 install
$ mvn -Dscala.compat.version=2.12 install
```

(The two `mvn` runs are to cross-compile the Scala SDK for Scala 2.11 and 2.12.)

### Building and Testing

You can test pretty easily:

```
$ mvn test
```

And building:

```
$ mvn -Dscala.compat.version=2.11 install
$ mvn -Dscala.compat.version=2.12 install
```

(You can always go into one of the sub-directories like `core-io` to only build or test an individual project.)

### Testing Infos

To cover all tests, the suite needs to be run against the following topologies:

 - 1 node, no replica
 - 2 nodes, 1 replica
 - 2 nodes, 2 replicas