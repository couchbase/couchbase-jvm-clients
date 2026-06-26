# Couchbase JVM Clients

Java, Scala, and Kotlin SDKs for server-side application development with Couchbase.

[![latest](https://img.shields.io/maven-central/v/com.couchbase.client/couchbase-client-bom?color=brightgreen&label=latest)](https://central.sonatype.com/artifact/com.couchbase.client/couchbase-client-bom)

## Getting started
* [Getting started with the Java SDK](https://docs.couchbase.com/java-sdk/current/hello-world/start-using-sdk.html)

* [Getting started with the Scala SDK](https://docs.couchbase.com/scala-sdk/current/hello-world/start-using-sdk.html)

* [Getting started with the Kotlin SDK](https://docs.couchbase.com/kotlin-sdk/current/hello-world/overview.html)

## Helpful resources

* [Couchbase Technical Support](https://www.couchbase.com/support) for Enterprise License subscribers.
* [Couchbase Developer Community](https://www.couchbase.com/developers/community/) for everyone!


## Building from source

Most developers should use [pre-built artifacts from Maven Central](https://central.sonatype.com/namespace/com.couchbase.client).
The following instructions are for developers who want to build the clients from source.

To build the clients, you will need:

* JDK 17 or later.
* `make` (Yes, it's a strange requirement for a Java project.)

Run these commands in your terminal.
```shell
git clone https://github.com/couchbase/couchbase-jvm-clients.git
cd couchbase-jvm-clients
make deps-only
./mvnw clean install
```

The `make deps-only` step is required after the initial checkout, and also when the project's shaded dependencies change.
To be on the safe side, run it after every pull.

### Building just one SDK

After running `make deps-only` at least once, you can build an individual client like this:

```shell script
./mvnw clean install --also-make --projects java-client
```

Add `-DskipTests` to skip the unit tests.


### Targeting different Scala versions

The Couchbase Scala SDK comes in different flavors for Scala 2.12, 2.13 and 3.3 LTS (which in turn supports apps built with Scala 3.3 through 3.7 inclusive).

Select the Scala flavor by activating **exactly one** of these Maven profiles:

* `scala-2` (for Scala 2.12; this is the default)
* `scala-2.13`
* `scala-3`

Example:

```shell
./mvnw clean install -Pscala-2.13
```

### IDE Configuration

#### IntelliJ
Scala code is automatically formatted on compile with the tool `scalafmt`.  To make IntelliJ use the same settings:

In Editor -> Code Style -> Scala:
* Change formatter to scalafmt.
* Click 'disable' next to the EditorConfig warning.
* Check [Reformat on file save](https://scalameta.org/scalafmt/docs/installation.html#format-on-save).

(Run `mvn validate` in the terminal to force reformat.)


## Branches and tags

This repository uses a unified versioning strategy.
Each component has the same version number, which is defined in `.mvn/maven.config`.

The `master` branch is where development for the next minor release happens.

Maintenance branches are named `<major>.<minor>.x`.
For example, the maintenance branch for minor version 3.9 is named `3.9.x`.

Instead of committing directly to a maintenance branch, first commit to `master` and then cherry-pick to the maintenance branch if possible.

### Historical branches

Before 3.9.0, components were versioned independently, and we named maintenance branches after supercomputers. This table shows the historical maintenance branches and the SDK versions associated with each branch.

| Branch     | core-io | java-client | scala-client | kotlin-client |
|------------|---------|-------------|--------------|---------------|
| `colossus`   | 2.0     | 3.0         | 1.0          |               |
| `pegasus`    | 2.1     | 3.1         | 1.1          |               |
| `hopper`     | 2.2     | 3.2         | 1.2          |               |
| `eos` *      | 2.3     | 3.3         | 1.3          | 1.0           |
| `frontier` * | 2.4     | 3.4         | 1.4          | 1.1           |
| `titan`      | 2.5     | 3.5         | 1.5          | 1.2           |
| `fugaku`     | 2.6     | 3.6         | 1.6          | 1.3           |
| `selene` †   | 3.7     | 3.7         | 1.7          | 1.4           |
| `aurora`     | 3.8     | 3.8         | 1.8          | 1.5           |
| `3.9.x` ‡    | 3.9     | 3.9         | 3.9          | 3.9           |

\* We did not create maintenenace branches for `eos` or `frontier`, but we did use these names in release tags.

† For `selene` we aligned the `core-io` version with the `java-client` version.

‡ Branch `3.9.x` is included in this table to show how we aligned the SDK versions and started naming the branch after the version.
All subsequent versions follow this pattern.

### Historical tags

Before 3.9.0, release tags included the component name, like `java-client-3.6.0` or `scala-client-1.6.0`.

