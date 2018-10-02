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
 
Regular Couchbase users will note that both the scala and the kotling binding are new! Yes they are, and we are trying to develop them as first-class citizens similar to the java client going forward.
 
## Usage

Since we are not publishing builds yet, you need to do a local checkout and build.

```
$ git clone https://github.com/daschl/couchbase-jvm-clients.git
$ cd couchbase-jvm-clients
$ ./gradlew publishToMavenLocal
```

### Building and Testing

You can test pretty easily:

```
$ ./gradlew test
```

And building:

```
$ ./gradlew build
```

(You can always go into one of the sub-directories like `core-io` to only build or test an individual project.)