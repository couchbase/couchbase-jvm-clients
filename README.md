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

Stable releases are pushed to maven central.

For Java:

```xml
<dependencies>
    <dependency>
        <groupId>com.couchbase.client</groupId>
        <artifactId>java-client</artifactId>
        <version>3.0.0</version>
    </dependency>
</dependencies>
```

For Scala:

```xml
<dependencies>
    <dependency>
        <groupId>com.couchbase.client</groupId>
        <artifactId>scala-client_2.12</artifactId>
        <version>1.0.0</version>
    </dependency>
</dependencies>
```
or if you use sbt:
```sbt
libraryDependencies += "com.couchbase.client" %% "scala-client" % "1.0.0"
```

## Building
You can always also just build it from source:

```
$ git clone https://github.com/couchbase/couchbase-jvm-clients.git
$ cd couchbase-jvm-clients
$ make
```

Yes, we need `make` because maven doesn't support the setup we need and neither does gradle. If you
want to build for different Scala versions, after the first `make` you can do this through:

```shell script
$ ./mvnw -Dscala.compat.version=2.13 -Dscala.compat.library.version=2.13.1 clean install
$ ./mvnw -Dscala.compat.version=2.11 -Dscala.compat.library.version=2.11.12 clean install
```

Notes:
+ The two `mvn` runs are to cross-compile the Scala SDK for Scala 2.11, and 2.13
+ Couchbase only officially provides, tests and supports a Scala 2.12 build.  
Our community kindly added the capability to create builds for Scala 2.11 and 2.13, and users are of course welcome to create such builds; but Couchbase cannot provide support for them.
+ When building for Scala 2.11, JDK 8 should be used. If JDK 11 is used then goal scala:doc-jar will fail
+ Default `scala.compat.`X properties are defined in file [.mvn/maven.config]
+ You can always go into one of the sub-directories like `core-io` to only build or test an 
individual project:
    ```shell script
    cd scala-client
    ../mvnw -DskipTests clean install
    ```
+ Use `-DskipTests` to skip testing.

### Testing 

You can test like this:

```shell script
$ ./mvnw clean test -fae
```

### Branches & Release Trains

Since this monorepo houses different versions of different artifacts, release train names have been chosen
to identify a collection of releases that belong to the same train.

These trains are named after historic computers for your delight.

Tags in each branch are named `branchname-ga` for the initial GA release, and then subsequently `branchname-sr-n` for
each service release. See the tag information for specifics of what's in there.

 - [Colossus](https://en.wikipedia.org/wiki/Colossus_computer) (Initial Release 2020-01-10)

| Release Train | Java-Client | Scala-Client | Core-Io | Tracing-Opentelemetry | Tracing-Opentracing |
| ------------- | ----------- | ------------ | ------- | --------------------- | ------------------- |
| colossus      | 3.0.x       | 1.0.x        | 2.0.x   | 0.2.x                 | 0.2.x               |

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
and check [Reformat on file save](https://scalameta.org/scalafmt/docs/installation.html#format-on-save)

(`mvn validate` can be used from command-line to force reformat)
