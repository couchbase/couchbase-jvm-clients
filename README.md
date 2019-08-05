# Couchbase JVM Clients

This repository contains the next generation of Couchbase SDKs ("3.0") as well as a rewritten 
`core-io` library.

It is currently in an `alpha` state and not ready for prime-time, although we are getting there.

## Overview

This repository contains the following projects:

 - `core-io`: the core library for all language bindings
 - `java-client`: the Java language binding
 - `scala-client`: the Scala language binding

And soon:

 - `kotlin-client`: the Kotlin language binding
 
Regular Couchbase users will note that both the Scala and the Kotlin binding are new! Yes they are, 
and we are trying to develop them as first-class citizens similar to the Java client going forward.
 
Documentation is now available for [Java](https://docs.couchbase.com/java-sdk/3.0/hello-world/start-using-sdk.html) 
and [Scala](https://docs.couchbase.com/scala-sdk/1.0/start-using-sdk.html)
 
## Usage

We've started to publish artifacts to our own maven repository:

```xml
<repositories>
    <repository>
      <id>couchbase</id>
      <name>Couchbase Preview Repository</name>
      <url>http://files.couchbase.com/maven2</url>
    </repository>
</repositories>
```

For Java:

```xml
<dependencies>
    <dependency>
        <groupId>com.couchbase.client</groupId>
        <artifactId>java-client</artifactId>
        <version>3.0.0-alpha.6</version>
    </dependency>
</dependencies>
```

For Scala:

```xml
<dependencies>
    <dependency>
        <groupId>com.couchbase.client</groupId>
        <artifactId>scala-client_2.12</artifactId>
        <version>1.0.0-alpha.6</version>
    </dependency>
</dependencies>
```

## Building
You can always also just build it from source:

```
$ git clone https://github.com/couchbase/couchbase-jvm-clients.git
$ cd couchbase-jvm-clients
$ make
```

Yes, we need make because maven doesn't support the setup we need and neither does gradle. If you
want to build for different scala versions, after the first `make` you can do this through:

```
$ mvn -Dscala.compat.version=2.12 -Dscala.compat.library.version=2.12.8 clean install
$ mvn -Dscala.compat.version=2.11 -Dscala.compat.library.version=2.11.12 clean install
```

(The two `mvn` runs are to cross-compile the Scala SDK for Scala 2.11 and 2.12.  Though note that the 2.11 build is not quite working yet.)

(You can always go into one of the sub-directories like `core-io` to only build or test an 
individual project.)

Use `-DskipTests` to skip testing.

### Testing 

You can test like this:

```
$ mvn --fail-at-end clean test
```

### Testing Infos

To cover all tests, the suite needs to be run against the following topologies, but by default it
runs against the mock. Recommended topologies:

 - 1 node, no replica
 - 2 nodes, 1 replica
 - 2 nodes, 2 replicas