
## Testing with FIT

(This documentation is for internal Couchbase developers.) 

Each SDK has its own FIT "performer" module (for example, `java-fit-performer`).
These modules are not included in the build by default as they require a bit of setup.

Tell your IDE about the performer modules by activating the `fit` Maven profile.
Reload / sync the Maven project configuration for the change to take effect.
See [Activating a Maven profile in IntelliJ IDEA](https://www.jetbrains.com/help/idea/work-with-maven-profiles.html#activate_maven_profiles).

Before each FIT testing session, generate (or refresh) the FIT protocol source code by right-clicking on the `FIT Protocol (gRPC + Protobuf)` module in the Maven tool window and selecting "Generate Sources and Update Folders".
Alternatively, if you prefer the command line:

    cd fit-performer-protocol
    ../mvnw clean protobuf:generate

Now you can run or debug `JavaPerfomer` / `ScalaPerformer` / `KotlinPerformer` from within your IDE like any other class.

### Updating to the latest protocol

The Protobuf files in `fit-performer-protocol` are manually mirrored from  `couchbaselabs/fit-protocol`.
To update the local mirror:

    cd fit-performer-protcol
    ./scripts/update-protobuf.sh

Then review and commit any changes.


### Cluster topologies

To cover all tests, the suite needs to be run against the following topologies, but by default it
runs against the mock. Recommended topologies:

- 1 node, no replica
- 2 nodes, 1 replica
- 2 nodes, 2 replicas

Also to have maximum service coverage use a cluster which has all services enabled (can be MDS setup).
