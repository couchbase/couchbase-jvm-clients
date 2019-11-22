# Couchbase JVM Clients

This repository contains the third generation of the Couchbase SDKs on the JVM ("3.0").

The SDKs are currently in a `beta` state, and should be stable to develop and experiment against. Please do not 
use them in production until we release them as `GA` (planned for January 2020).

## Overview

This repository contains the following projects:

 - `core-io`: the core library for all language bindings
 - `java-client`: the Java language binding
 - `scala-client`: the Scala language binding

You'll also find utility libraries and integration components at the toplevel (i.e for tracing).
 
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
        <version>3.0.0-beta.2</version>
    </dependency>
</dependencies>
```

For Scala:

```xml
<dependencies>
    <dependency>
        <groupId>com.couchbase.client</groupId>
        <artifactId>scala-client_2.12</artifactId>
        <version>1.0.0-beta.1</version>
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

Yes, we need `make` because maven doesn't support the setup we need and neither does gradle. If you
want to build for different scala versions, after the first `make` you can do this through:

```
$ mvn -Dscala.compat.version=2.12 -Dscala.compat.library.version=2.12.8 clean install
$ mvn -Dscala.compat.version=2.11 -Dscala.compat.library.version=2.11.12 clean install
```

(The two `mvn` runs are to cross-compile the Scala SDK for Scala 2.11 and 2.12.

(You can always go into one of the sub-directories like `core-io` to only build or test an 
individual project.)

Use `-DskipTests` to skip testing.

### Testing 

You can test like this:

```
$ mvn clean test -fae
```

### Testing Infos

To cover all tests, the suite needs to be run against the following topologies, but by default it
runs against the mock. Recommended topologies:

 - 1 node, no replica
 - 2 nodes, 1 replica
 - 2 nodes, 2 replicas
 
## IDE Configuration

### IntelliJ
Scala code is automatically formatted on compile with the tool `scalafmt`.  To make IntelliJ use the same settings:

Editor -> Code Style -> Scala, change formatter to scalafmt

(`mvn validate` can be used from command-line to force reformat)