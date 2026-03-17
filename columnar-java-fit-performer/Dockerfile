# A Docker image for the Couchbase Columnar SDK test performer for Java.
#
# NOTE: This is a "legacy" Dockerfile for the buildPerformer script in jenkins-sdk.
#       New workflows should use the unified Dockerfile at the project root.
#
# To build:
#   1. Uncomment the fit-performer modules in the parent POM.
#   2. cd to the directory containing couchbase-jvm-clients, then run:
#        docker build . -f couchbase-jvm-clients/java-fit-performer/Dockerfile
#
# To run:
#   docker run -e LOG_LEVEL=DEBUG -p 8060:8060


# Valid SDK values: java, scala, kotlin, columnar-java
ARG SDK=columnar-java

FROM maven:3.9.6-eclipse-temurin-21 AS build

WORKDIR /app
COPY couchbase-jvm-clients couchbase-jvm-clients/

WORKDIR /app/couchbase-jvm-clients
ARG MVN_FLAGS="--batch-mode --no-transfer-progress -Dcheckstyle.skip -Dmaven.test.skip -Dmaven.javadoc.skip"
RUN mvn $MVN_FLAGS -f protostellar/pom.xml clean install
RUN mvn $MVN_FLAGS -f core-io-deps/pom.xml clean install
RUN mvn $MVN_FLAGS -f tracing-opentelemetry-deps/pom.xml clean install

# Defer declaring the ARG until first use, so the previous layers can be cached between SDKs
ARG SDK
RUN mvn $MVN_FLAGS package --projects ${SDK}-fit-performer --also-make

# Multistage build to keep things small
FROM eclipse-temurin:21-jre-ubi10-minimal

WORKDIR /app

ARG SDK
COPY --from=build /app/couchbase-jvm-clients/${SDK}-fit-performer/target/${SDK}-fit-performer-1.0-SNAPSHOT-jar-with-dependencies.jar performer.jar

ENV LOG_LEVEL=INFO
EXPOSE 8060

ENTRYPOINT ["java", "-jar", "performer.jar"]
