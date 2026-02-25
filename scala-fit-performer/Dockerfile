# A Docker image for the Couchbase SDK performance test performer for Scala.
# Build from root with:
# docker build -f couchbase-jvm-clients/scala-fit-performer/Dockerfile .

FROM maven:3.8.1-openjdk-17
WORKDIR /app
ARG MVN_FLAGS="-T6 -B -ntp -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn"

COPY transactions-fit-performer transactions-fit-performer/
COPY couchbase-jvm-clients couchbase-jvm-clients/

WORKDIR /app/transactions-fit-performer
RUN mvn $MVN_FLAGS --projects gRPC/jvm --also-make install -Dcheckstyle.skip -Dmaven.test.skip -Dmaven.javadoc.skip

WORKDIR /app/couchbase-jvm-clients
RUN mvn $MVN_FLAGS -f protostellar/pom.xml clean install -Dcheckstyle.skip -Dmaven.test.skip -Dmaven.javadoc.skip
RUN mvn $MVN_FLAGS -f core-io-deps/pom.xml clean install -Dcheckstyle.skip -Dmaven.test.skip -Dmaven.javadoc.skip
RUN mvn $MVN_FLAGS -f tracing-opentelemetry-deps/pom.xml clean install
RUN mvn $MVN_FLAGS package -pl scala-fit-performer -am -Dcheckstyle.skip -Dmaven.test.skip -Dmaven.javadoc.skip

# Multistage build to keep things small
FROM maven:3.8.1-openjdk-17

WORKDIR /app
# Default level; override with docker run -e LOG_LEVEL=DEBUG
ENV LOG_LEVEL=INFO

COPY --from=0 /app/couchbase-jvm-clients/scala-fit-performer/target/scala-fit-performer-1.0-SNAPSHOT-jar-with-dependencies.jar .
ENTRYPOINT ["java", "-jar", "scala-fit-performer-1.0-SNAPSHOT-jar-with-dependencies.jar"]
